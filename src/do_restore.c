#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"
#include "restore.h"

static void* lru_restore_thread(void *arg) {
	struct lruCache *cache;
	if (destor.simulation_level >= SIMULATION_RESTORE)
		cache = new_lru_cache(destor.restore_cache[1], free_container_meta,
				lookup_fingerprint_in_container_meta);
	else
		cache = new_lru_cache(destor.restore_cache[1], free_container,
				lookup_fingerprint_in_container);

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(restore_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (destor.simulation_level >= SIMULATION_RESTORE) {
			struct containerMeta *cm = lru_cache_lookup(cache, &c->fp);
			if (!cm) {
				VERBOSE("Restore cache: container %lld is missed", c->id);
				cm = retrieve_container_meta_by_id(c->id);
				assert(lookup_fingerprint_in_container_meta(cm, &c->fp));
				lru_cache_insert(cache, cm, NULL, NULL);
				jcr.read_container_num++;
			}

			TIMER_END(1, jcr.read_chunk_time);
		} else {
			struct container *con = lru_cache_lookup(cache, &c->fp);
			if (!con) {
				VERBOSE("Restore cache: container %lld is missed", c->id);
				TIMER_DECLARE(2);
				TIMER_BEGIN(2);
				con = retrieve_container_by_id(c->id);
				TIMER_END(2, jcr.read_container_time);
				lru_cache_insert(cache, con, NULL, NULL);
				jcr.read_container_num++;
			}
			struct chunk *rc = get_chunk_in_container(con, &c->fp);
			assert(rc);
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_push(restore_chunk_queue, rc);
		}

		jcr.data_size += c->size;
		jcr.chunk_num++;
		free_chunk(c);
	}

	sync_queue_term(restore_chunk_queue);

	free_lru_cache(cache);

	return NULL;
}

static void* read_recipe_thread(void *arg) {

	int i, j, k;
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		//每次循环处理一个文件
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);
		sync_queue_push(restore_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);
			if (destor.restore_cache[0] != RESTORE_CACHE_ASM_LOCALITY) {
				//原版destor代码块
				//一次读取一个配方单元chunkPointer
				struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);

				//创建无数据大小的chunk来存放配方中的信息
				struct chunk* c = new_chunk(0);
				memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
				c->size = cp->size;
				c->id = cp->id;

				TIMER_END(1, jcr.read_recipe_time);

				// restore_recipe_queue中只会有file开始结束标记，没有segment开始结束标记
				sync_queue_push(restore_recipe_queue, c);
				free(cp);
			}
			else {
				//考虑局部性时
				//一次读取一个配方单元chunkPointer
				struct chunkPointerForLocality* cp = 
					read_next_n_chunk_pointers_forLocality(jcr.bv, 1, &k);

				//创建无数据大小的chunk来存放配方中的信息
				struct chunk* c = new_chunk(0);
				memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
				c->size = cp->size;
				c->id = cp->id;
				c->pre = cp->pre;
				c->next = cp->next;

				TIMER_END(1, jcr.read_recipe_time);

				// restore_recipe_queue中只会有file开始结束标记，没有segment开始结束标记
				sync_queue_push(restore_recipe_queue, c);
				free(cp);
			}
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(restore_recipe_queue, c);

		free_file_recipe_meta(r);
	}

	sync_queue_term(restore_recipe_queue);
	return NULL;
}

void* write_restore_data(void* arg) {

	char *p, *q;
	q = jcr.path + 1;/* ignore the first char*/
	/*
	 * recursively make directory
	 */
	while ((p = strchr(q, '/'))) {
		if (*p == *(p - 1)) {
			q++;
			continue;
		}
		*p = 0;
		if (access(jcr.path, 0) != 0) {
			mkdir(jcr.path, S_IRWXU | S_IRWXG | S_IRWXO);
		}
		*p = '/';
		q = p + 1;
	}

	struct chunk *c = NULL;
	FILE *fp = NULL;

	while ((c = sync_queue_pop(restore_chunk_queue))) {
		//自己的模式
		//当destor.restore_cache[3]为0时，即使设置为SIMULATION_NO，恢复的时候也不会写入真实数据，模拟数据以网络形式发出去
		if (destor.restore_cache[3] == 0) {
			free_chunk(c);
			continue;
		}
		
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
			VERBOSE("Restoring: %s", c->data);

			sds filepath = sdsdup(jcr.path);
			filepath = sdscat(filepath, c->data);

			int len = sdslen(jcr.path);
			char *q = filepath + len;
			char *p;
			while ((p = strchr(q, '/'))) {
				if (*p == *(p - 1)) {
					q++;
					continue;
				}
				*p = 0;
				if (access(filepath, 0) != 0) {
					mkdir(filepath, S_IRWXU | S_IRWXG | S_IRWXO);
				}
				*p = '/';
				q = p + 1;
			}

			if (destor.simulation_level == SIMULATION_NO) {
				assert(fp == NULL);
				fp = fopen(filepath, "w");
			}

			sdsfree(filepath);

		}
		else if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
		    jcr.file_num++;

			if (fp)
				fclose(fp);
			fp = NULL;
		}
		else {
			assert(destor.simulation_level == SIMULATION_NO);
			VERBOSE("Restoring %d bytes", c->size);
			fwrite(c->data, c->size, 1, fp);
		}

		free_chunk(c);

		TIMER_END(1, jcr.write_chunk_time);
	}

    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

void do_restore(int revision, char *path) {

	//  读取/recipes/backupversion.count文件，得到backup_version_count数据
	init_recipe_store();
	//  读取/container.pool文件，得到container_count数据
	init_container_store();
	//  初始化jcr和backupVersion
	init_restore_jcr(revision, path);

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "restore to: %s", jcr.path);

	restore_chunk_queue = sync_queue_new(1000);
	restore_recipe_queue = sync_queue_new(1000);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== restore begin ====");

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t, write_t;
	//读取文件配方线程
	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);

	if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
		destor_log(DESTOR_NOTICE, "restore cache is LRU");
		pthread_create(&read_t, NULL, lru_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {
		destor_log(DESTOR_NOTICE, "restore cache is OPT");
		pthread_create(&read_t, NULL, optimal_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {
		destor_log(DESTOR_NOTICE, "restore cache is ASM");
		pthread_create(&read_t, NULL, assembly_restore_thread, NULL);
	}else if (destor.restore_cache[0] == RESTORE_CACHE_ASM_LOCALITY) {
		destor_log(DESTOR_NOTICE, "restore cache is ASM-LOCALITY");
		pthread_create(&read_t, NULL, locality_assembly_restore_thread, NULL);
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_MY_ASM) {
		destor_log(DESTOR_NOTICE, "restore cache is MY-ASM");
		pthread_create(&read_t, NULL, enhance_assembly_restore_thread, NULL);

	}
	else{
		fprintf(stderr, "Invalid restore cache.\n");
		exit(1);
	}
	//写入恢复的文件数据线程
	pthread_create(&write_t, NULL, write_restore_data, NULL);

    do{
        sleep(1);
        /*time_t now = time(NULL);*/
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num);

	assert(sync_queue_size(restore_chunk_queue) == 0);
	assert(sync_queue_size(restore_recipe_queue) == 0);

	free_backup_version(jcr.bv);

	TIMER_END(1, jcr.total_time);
	puts("==== restore end ====");

	printf("job id: %" PRId32 "\n", jcr.id);
	printf("restore path: %s\n", jcr.path);
	printf("number of files: %" PRId32 "\n", jcr.file_num);
	printf("number of chunks: %" PRId32"\n", jcr.chunk_num);
	printf("total size(B): %" PRId64 "\n", jcr.data_size);
	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("throughput(MB/s): %.2f\n",
			jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
	printf("speed factor: %.2f\n",
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));
	printf("read container number: %d\n", jcr.read_container_num);
			
	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", (jcr.read_chunk_time - jcr.system_sync_drop_caches) / 1000000,
			jcr.data_size * 1000000 / (jcr.read_chunk_time - jcr.system_sync_drop_caches) / 1024 / 1024);
	printf("read_container_time : %.3fs, computing time = %.3fs\n",
		(jcr.read_container_time - jcr.system_sync_drop_caches) / 1000000,
		(jcr.read_chunk_time - jcr.read_container_time) / 1000000);
	printf("enhance_asm_expel_cache_time : %d, enhance_asm_cache_time : %d, expel_time / total_time : %.3f\n",
		jcr.enhance_asm_expel_cache_time,
		jcr.enhance_asm_cache_time,
		jcr.enhance_asm_expel_cache_time / (double) jcr.enhance_asm_cache_time);
	printf("write_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.write_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_chunk_time / 1024 / 1024);

	char* logfile = NULL;
	char* recipelog = NULL;
	char restore_asm[] = "restore_asm.log";
	char restore_lru[] = "restore_lru.log";
	char restore_opt[] = "restore_opt.log";
	char restore_enhance_asm[] = "restore_my.log";

	char recipe_asm[] = "recipe_asm.log";
	char recipe_enhance_asm[] = "recipe_my.log";
	if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
		logfile = restore_lru;
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {
		logfile = restore_opt;
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {
		logfile = restore_asm;
		recipelog = recipe_asm;
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_MY_ASM) {
		logfile = restore_enhance_asm;
		recipelog = recipe_enhance_asm;
	}

	FILE *fp = fopen(logfile, "a");
	FILE *fp2 = NULL;
	if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {
		fprintf(fp, "ASM %d : ", destor.restore_cache[1]);
		fp2 = fopen(recipelog, "a");
		fprintf(fp2, "maxRecipeNum : %ld, maxRecipeSize(byte) : %ld\n", jcr.max_recipe_num, jcr.max_recipe_num * 32);
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_MY_ASM) {
		fprintf(fp, "MY_ASM %d %d : ", destor.restore_cache[1], destor.restore_cache[2]);
		fp2 = fopen(recipelog, "a");
		fprintf(fp2, "maxRecipeNum : %ld, maxRecipeSize(byte) : %ld\n", jcr.max_recipe_num, jcr.max_recipe_num * 32);
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
		fprintf(fp, "LRU %d : ", destor.restore_cache[1]);
	}
	else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {
		fprintf(fp, "OPT %d %d : ", destor.restore_cache[1], destor.restore_opt_window_size);
	}



	/*
	 * job id,
	 * data size,
	 * actually read container number,
	 * speed factor,
	 * restore throughput
	 * 二级缓存逐出频率
	 * 恢复过程耗时
	 * 读取容器耗时
	 * 恢复过程中的计算耗时
	 */
	fprintf(fp, "%" PRId32 " %.2f %" PRId32 " %.4f %.4f %.3f %.3f %.3f %.3f\n",
		jcr.id, 
		jcr.data_size / 1024.0 / 1024.0,
		jcr.read_container_num,
		jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
		jcr.data_size * 1000000 / (jcr.read_chunk_time - jcr.system_sync_drop_caches) / 1024 / 1024,
		jcr.enhance_asm_cache_time != 0 ? jcr.enhance_asm_expel_cache_time / (double)jcr.enhance_asm_cache_time
			: 0 ,
		(jcr.read_chunk_time - jcr.system_sync_drop_caches) / 1000000,
		(jcr.read_container_time - jcr.system_sync_drop_caches) / 1000000,
		(jcr.read_chunk_time - jcr.read_container_time) / 1000000);

	fclose(fp);

	close_container_store();
	close_recipe_store();
}

