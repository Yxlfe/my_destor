#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t hash_t;
static int64_t chunk_num;

/* 计算SHA1函数 */
static void* sha1_thread(void* arg) {
	char code[41];
	while (1) {
		struct chunk* c = sync_queue_pop(chunk_queue);

		if (c == NULL) {
			sync_queue_term(hash_queue);
			break;
		}
		//chunk c 为文件流开始标记块或者结束块时，直接将c放入hash_queue中
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(hash_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		SHA_CTX ctx;
		SHA_Init(&ctx);
		SHA_Update(&ctx, c->data, c->size);
		SHA_Final(c->fp, &ctx);	//计算SHA1并写入chunk 的fp域
		TIMER_END(1, jcr.hash_time);

		hash2code(c->fp, code);
		code[40] = 0;
		VERBOSE("Hash phase: %ldth chunk identified by %s", chunk_num++, code);

		//将c放入hash_queue中
		sync_queue_push(hash_queue, c);
	}
	return NULL;
}

void start_hash_phase() {
	//创建存放chunk的同步队列，最大容量为100
	hash_queue = sync_queue_new(100);
	pthread_create(&hash_t, NULL, sha1_thread, NULL);
}

void stop_hash_phase() {
	pthread_join(hash_t, NULL);
	NOTICE("hash phase stops successfully!");
}
