#include "destor.h"
#include "jcr.h"
#include "chunking/chunking.h"
#include "backup.h"
#include "storage/containerstore.h"

static pthread_t chunk_t;
static int64_t chunk_num;

static int (*chunking)(unsigned char* buf, int size);

static inline int fixed_chunk_data(unsigned char* buf, int size){
	return destor.chunk_avg_size > size ? size : destor.chunk_avg_size;
}

/*
 * chunk-level deduplication.
 * Destor currently supports fixed-sized chunking and (normalized) rabin-based chunking.
 */
static void* chunk_thread(void *arg) {
	int leftlen = 0;
	int leftoff = 0;
	//创建大小为 1M+64M 的缓冲区，防止切块后继续读超出缓冲区大小
	unsigned char *leftbuf = malloc(DEFAULT_BLOCK_SIZE + destor.chunk_max_size);

	unsigned char *zeros = malloc(destor.chunk_max_size);
	bzero(zeros, destor.chunk_max_size);	//zeros指向的内存全部清零
	unsigned char *data = malloc(destor.chunk_max_size);

	struct chunk* c = NULL;

	//一次循环处理一个文件的所有数据
	while (1) {
		/* Try to receive a CHUNK_FILE_START. */
		c = sync_queue_pop(read_queue);

		if (c == NULL) {
			sync_queue_term(chunk_queue);
			break;
		}

		/*	#define CHECK_CHUNK(c, f) (c->flag & f)
		    检查文件流chunk的flag是否为CHUNK_FILE_START
			然后将文件流开始标记的chunk放入chunk_queue队列
		 */
		assert(CHECK_CHUNK(c, CHUNK_FILE_START));
		sync_queue_push(chunk_queue, c);

		/* Try to receive normal chunks. */
		c = sync_queue_pop(read_queue);
		if (!CHECK_CHUNK(c, CHUNK_FILE_END)) {
			memcpy(leftbuf, c->data, c->size);
			leftlen += c->size;
			free_chunk(c);
			c = NULL;
		}

		while (1) {
			/* c == NULL indicates more data for this file can be read. */
			while ((leftlen < destor.chunk_max_size) && c == NULL) {
				c = sync_queue_pop(read_queue);
				if (!CHECK_CHUNK(c, CHUNK_FILE_END)) {
					//剩下未分块的数据向前移
					memmove(leftbuf, leftbuf + leftoff, leftlen); 
					leftoff = 0;	//偏移量置为0
					memcpy(leftbuf + leftlen, c->data, c->size);
					leftlen += c->size;
					free_chunk(c);
					c = NULL;
				}
			} //每次都读满64KB或者到达文件流结束位置

			if (leftlen == 0) {
				assert(c);
				break;
			}

			TIMER_DECLARE(1);
			TIMER_BEGIN(1);
			//分块算法后返回的块大小
			int	chunk_size = chunking(leftbuf + leftoff, leftlen);

			TIMER_END(1, jcr.chunk_time);
			//创建chunk_size大小的新块
			struct chunk *nc = new_chunk(chunk_size);
			memcpy(nc->data, leftbuf + leftoff, chunk_size);
			leftlen -= chunk_size;
			leftoff += chunk_size;

			if (memcmp(zeros, nc->data, chunk_size) == 0) {
				//相等的情况
				VERBOSE("Chunk phase: %ldth chunk  of %d zero bytes",
						chunk_num++, chunk_size);
				jcr.zero_chunk_num++;
				jcr.zero_chunk_size += chunk_size;
			} else
				VERBOSE("Chunk phase: %ldth chunk of %d bytes", chunk_num++,
						chunk_size);
			//将切分的块放入队列chunk_queue
			sync_queue_push(chunk_queue, nc);
		}
		//将文件流结束标志的chunk放入chunk_queue队列
		sync_queue_push(chunk_queue, c);
		leftoff = 0;
		c = NULL;

		if(destor.chunk_algorithm == CHUNK_RABIN ||
				destor.chunk_algorithm == CHUNK_NORMALIZED_RABIN)
			windows_reset();

	}

	free(leftbuf);
	free(zeros);
	free(data);
	return NULL;
}



void start_chunk_phase() {

	if (destor.chunk_algorithm == CHUNK_RABIN){
		//分块算法为Rabin算法的时候

		/* 将chunck_avg_size向下处理为2^n */
		int pwr;
		for (pwr = 0; destor.chunk_avg_size; pwr++) {
			destor.chunk_avg_size >>= 1;
		}
		destor.chunk_avg_size = 1 << (pwr - 1);

		assert(destor.chunk_avg_size >= destor.chunk_min_size);
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunkAlg_init();
		//chunking函数指针指向rabin_chunk_data()函数
		chunking = rabin_chunk_data;

	}else if(destor.chunk_algorithm == CHUNK_NORMALIZED_RABIN){
		int pwr;
		for (pwr = 0; destor.chunk_avg_size; pwr++) {
			destor.chunk_avg_size >>= 1;
		}
		destor.chunk_avg_size = 1 << (pwr - 1);

		assert(destor.chunk_avg_size >= destor.chunk_min_size);
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunkAlg_init();
		chunking = normalized_rabin_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_TTTD){
		int pwr;
		for (pwr = 0; destor.chunk_avg_size; pwr++) {
			destor.chunk_avg_size >>= 1;
		}
		destor.chunk_avg_size = 1 << (pwr - 1);

		assert(destor.chunk_avg_size >= destor.chunk_min_size);
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunkAlg_init();
		chunking = tttd_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_FIXED){
		assert(destor.chunk_avg_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		destor.chunk_max_size = destor.chunk_avg_size;
		chunking = fixed_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_FILE){
		/*
		 * approximate file-level deduplication
		 * It splits the stream according to file boundaries.
		 * For a larger file, we need to split it due to container size limit.
		 * Hence, our approximate file-level deduplication are only for files smaller than CONTAINER_SIZE - CONTAINER_META_SIZE.
		 * Similar to fixed-sized chunking of $(( CONTAINER_SIZE - CONTAINER_META_SIZE )) chunk size.
		 * */
		destor.chunk_avg_size = CONTAINER_SIZE - CONTAINER_META_SIZE;
		destor.chunk_max_size = CONTAINER_SIZE - CONTAINER_META_SIZE;
		chunking = fixed_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_AE){
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunking = ae_chunk_data;
		ae_init();
	}else{
		NOTICE("Invalid chunking algorithm");
		exit(1);
	}

	//创建存放chunk的同步队列，最大容量为100
	chunk_queue = sync_queue_new(100);
	pthread_create(&chunk_t, NULL, chunk_thread, NULL);
}

void stop_chunk_phase() {
	pthread_join(chunk_t, NULL);
	NOTICE("chunk phase stops successfully!");
}
