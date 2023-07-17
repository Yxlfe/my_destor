#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t read_t;

static void read_file(sds path) {
	//1M的缓冲区
	static unsigned char buf[DEFAULT_BLOCK_SIZE];

	sds filename = sdsdup(path);

	if (jcr.path[sdslen(jcr.path) - 1] == '/') {
		/* the backup path points to a direcory */
		sdsrange(filename, sdslen(jcr.path), -1);
	} else {
		/* the backup path points to a file */
		int cur = sdslen(filename) - 1;
		while (filename[cur] != '/')
			cur--;
		sdsrange(filename, cur, -1);
	}

	FILE *fp;
	if ((fp = fopen(path, "r")) == NULL) {
		destor_log(DESTOR_WARNING, "Can not open file %s\n", path);
		perror("The reason is");
		exit(1);
	}
	//创建文件流开始标志chunk，将filename 设置到data中
	struct chunk *c = new_chunk(sdslen(filename) + 1);
	strcpy(c->data, filename);

	VERBOSE("Read phase: %s", filename);

	//#define SET_CHUNK(c, f) (c->flag |= f) 将chunk的flag和f异或后再赋给flag
	/* 将chunk 的flag设置为文件开始标记
		将chunk放入同步队列read_queue中 */
	SET_CHUNK(c, CHUNK_FILE_START);
	sync_queue_push(read_queue, c);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	int size = 0;

	while ((size = fread(buf, 1, DEFAULT_BLOCK_SIZE, fp)) != 0) {
		TIMER_END(1, jcr.read_time);

		VERBOSE("Read phase: read %d bytes", size);

		/* 新建正常数据流chunk，大小为已读进缓存区数据大小size(最大为1M），
			并将数据拷贝到chunk中的data域，
			再将chunk放入同步队列read_queue中 */
		c = new_chunk(size);
		memcpy(c->data, buf, size);
		sync_queue_push(read_queue, c);

		TIMER_BEGIN(1);
	}
	/* 创建文件流结束标志chunk，将flag设为文件结束标记，
		并放入同步队列read_queue */
	c = new_chunk(0);
	SET_CHUNK(c, CHUNK_FILE_END);
	sync_queue_push(read_queue, c);

	fclose(fp);

	sdsfree(filename);
}

static void find_one_file(sds path) {
	
	if (strcmp(path + sdslen(path) - 1, "/") == 0) {
		//如果路径下是目录，则进入if

		DIR *dir = opendir(path);
		struct dirent *entry;

		while ((entry = readdir(dir)) != 0) {
			/*ignore . and ..*/
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;
			sds newpath = sdsdup(path);
			newpath = sdscat(newpath, entry->d_name);

			struct stat state;
			if (stat(newpath, &state) != 0) {
				WARNING("The file %s does not exist! ignored!", newpath);
				return;
			}

			if (S_ISDIR(state.st_mode)) {
				assert(strcmp(newpath + sdslen(newpath) - 1, "/") != 0);
				newpath = sdscat(newpath, "/");
			}

			find_one_file(newpath);

			sdsfree(newpath);
		}

		closedir(dir);
	} else {
		//路径下就是文件本身
		read_file(path);
	}
}

static void* read_thread(void *argv) {
	/* Each file will be processed separately */
	find_one_file(jcr.path);
	sync_queue_term(read_queue);
	return NULL;
}

void start_read_phase() {
    /* running job */
    jcr.status = JCR_STATUS_RUNNING;
	//创建最大容量为10的同步队列
	read_queue = sync_queue_new(10);
	//创建新线程运行read_thread()函数
	pthread_create(&read_t, NULL, read_thread, NULL);
}

void stop_read_phase() {
	//以阻塞的方式等待read_thread线程结束
	pthread_join(read_t, NULL);
	NOTICE("read phase stops successfully!");
}

