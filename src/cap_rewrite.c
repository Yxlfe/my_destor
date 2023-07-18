#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"

static int64_t chunk_num;

//存放满足阈值条件的容器id
static GHashTable *top;
//存放串接信息的hashtable：key为第N个块，value为结构体connect_info；
static GHashTable *for_connect;

static void cap_segment_get_top() {

	/* Descending order */
	//按照containerRecord.size大小降序排序container_record_seq
	g_sequence_sort(rewrite_buffer.container_record_seq,
			g_record_descmp_by_length, NULL);

	int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
	int32_t num = length > destor.rewrite_capping_level ?
					destor.rewrite_capping_level : length, i;
	GSequenceIter *iter = g_sequence_get_begin_iter(
			rewrite_buffer.container_record_seq);
	//将前num个放入top哈希表中
	for (i = 0; i < num; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct containerRecord* record = g_sequence_get(iter);
		struct containerRecord* r = (struct containerRecord*) malloc(
				sizeof(struct containerRecord));
		memcpy(r, record, sizeof(struct containerRecord));
		//将out_of_order标记为0，表示未超出阈值范围
		r->out_of_order = 0;
		g_hash_table_insert(top, &r->cid, r);
		iter = g_sequence_iter_next(iter);
	}

	VERBOSE("Rewrite phase: Select Top-%d in %d containers", num, length);
	//恢复原有顺序，即按照containerRecord.cid大小排序container_record_seq
	g_sequence_sort(rewrite_buffer.container_record_seq, g_record_cmp_by_id, NULL);
}


static void cap_segment_get_top_fix() {
	/* Descending order */
	//按照containerRecordForCapping.chunk_number大小降序排序container_record_seq
	g_sequence_sort(rewrite_buffer.container_record_seq,
		g_recordForCapping_cmp_by_chunkNumber, NULL);

	int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
	int32_t num = length > destor.rewrite_capping_level ?
		destor.rewrite_capping_level : length, i;
	GSequenceIter *iter = g_sequence_get_begin_iter(
		rewrite_buffer.container_record_seq);
	//将前num个放入top哈希表中
	for (i = 0; i < num; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct containerRecordForCapping* record = g_sequence_get(iter);
		struct containerRecordForCapping* r = (struct containerRecordForCapping*) malloc(
			sizeof(struct containerRecordForCapping));
		memcpy(r, record, sizeof(struct containerRecordForCapping));
		//将out_of_order标记为0，表示未超出阈值范围
		r->out_of_order = 0;
		g_hash_table_insert(top, &r->physical_address_start, r);
		iter = g_sequence_iter_next(iter);
	}

	VERBOSE("Rewrite phase: Select Top-%d in %d containers", num, length);
	//恢复原有顺序，即按照containerRecord.cid大小排序container_record_seq
	//g_sequence_sort(rewrite_buffer.container_record_seq, g_recordForCapping_cmp_by_startAddress, NULL);
}


/*
在考虑容器局部性的思想下，得到满足阈值的所有容器id
*/
static void spatial_locality_cap_segment_get_top_fix() {

	/*以具有局部性的容器为段，根据每个段中所有块的数量逆序排序*/

	int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
	int i;int64_t address = -2000;
	GSequenceIter* iter = g_sequence_get_begin_iter(
		rewrite_buffer.container_record_seq);
	//创建存放localityContainerRecord的序列
	GSequence *localityContainerRecord_sequence = g_sequence_new(free);
	struct localityContainerRecord* lcRecordPoint = NULL;
	for (i = 0; i < length; i++) {
		struct containerRecordForCapping* fRecord = g_sequence_get(iter);
//		if (fRecord->physical_address_start - address != 1024) {
		if (fRecord->physical_address_start - address != 1) {
			if (lcRecordPoint != NULL) {
				//把前一个局部段对应的localityContainerRecord放入序列
				g_sequence_insert_sorted(localityContainerRecord_sequence,
					lcRecordPoint, g_localityContainerRecord_cmp_by_number, NULL);
			}
			//创建新的localityContainerRecord，更新lcRecordPoint指针
			lcRecordPoint = malloc(sizeof(struct localityContainerRecord));
			lcRecordPoint->containerRecordForCapping_queue = g_queue_new();
			lcRecordPoint->chunk_number = 0;
		}
		address = fRecord->physical_address_start;
		g_queue_push_tail(lcRecordPoint->containerRecordForCapping_queue, fRecord);
		lcRecordPoint->chunk_number += fRecord->chunk_number;
		iter = g_sequence_iter_next(iter);
	}
	if (lcRecordPoint != NULL) {
		//把最后一个局部段对应的localityContainerRecord放入序列
		g_sequence_insert_sorted(localityContainerRecord_sequence,
			lcRecordPoint, g_localityContainerRecord_cmp_by_number, NULL);
	}

	/*根据阈值将满足条件的前n个段中的容器记录放入top哈希表*/
	int length2 = g_sequence_get_length(localityContainerRecord_sequence);
	int32_t num = length2 > destor.rewrite_capping_level ?
		destor.rewrite_capping_level : length2;
	int j;
	GSequenceIter *iter2 = g_sequence_get_begin_iter(localityContainerRecord_sequence);
	//将前num个放入top哈希表中
	for (j = 0; j < num; j++) {
		assert(!g_sequence_iter_is_end(iter2));
		struct localityContainerRecord* cRecord = g_sequence_get(iter2);
		struct containerRecordForCapping* fRecord = NULL;
		int32_t pre = -1;
		struct connect_info* info = NULL;
		while (fRecord = g_queue_pop_head(cRecord->containerRecordForCapping_queue)) {
			struct containerRecordForCapping* r = 
				(struct containerRecordForCapping*) malloc(
					sizeof(struct containerRecordForCapping));
			memcpy(r, fRecord, sizeof(struct containerRecordForCapping));
			//将out_of_order标记为0，表示未超出阈值范围
			r->out_of_order = 0;
			g_hash_table_insert(top, &r->physical_address_start, r);

			if (info != NULL) {
				//为前一个设置next域，并将其放入for_connect哈希表
				info->next = fRecord->firstChunkNum_isContainer;
				g_hash_table_insert(for_connect, &info->index_number, info);
			}
			info = (struct connect_info*) malloc(sizeof(struct connect_info));
			info->index_number = fRecord->firstChunkNum_isContainer;
			info->pre = pre;
			pre = fRecord->firstChunkNum_isContainer;
			//next暂时默认为-1
			info->next = -1;
		}
		//把最后一个有实际意义的connect_info放入哈希表
		if(info->pre != -1 || info->next != -1){
			g_hash_table_insert(for_connect, &info->index_number, info);
		}

		iter2 = g_sequence_iter_next(iter2);
	}
	/*==========注意是否将localityContainerRecord_sequence中的所有元素的内存以及
				子内存都释放掉
	*/
	g_sequence_remove_range(g_sequence_get_begin_iter(localityContainerRecord_sequence),
		g_sequence_get_end_iter(localityContainerRecord_sequence));

	VERBOSE("Rewrite phase: Select Top-%d in %d containers", num, length2);
}


/*
考虑空间局部性的capping方法
*/
void *spatial_locality_cap_rewrite(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
	for_connect = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, free);

	while (1) {
		//查看队头元素
		struct chunk *c = sync_queue_get_top(dedup_queue);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_pushForCapping(c)) {
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_pop(dedup_queue);
			continue;
		}

		//用来显示区域各容器第一次出现的顺序
		printf("===========once stop analyse area!!!!!!\n");

		//在考虑容器局部性的思想下，得到满足阈值的所有容器id
		spatial_locality_cap_segment_get_top_fix();
		
		printf("connect_info hashtable information is :\n");
		GList* listValues = g_hash_table_get_values(for_connect);
		GList* start = g_list_first(listValues);
		struct connect_info* info = NULL;
		while(start != NULL ){
			info = start->data;
			printf("info.index_number = %d   //", info->index_number);
			printf("info.pre = %d   //", info->pre);
			printf("info.next = %d   // \n", info->next);
			start = start->next;
		}
		
		//记录rewrite_buffer.num最大值
		int max = rewrite_buffer.num;
		while ((c = rewrite_buffer_popForCapping())) {
			if (!CHECK_CHUNK(c, CHUNK_FILE_START)
				&& !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
				&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->physical_address_start) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
				}
				else {
					int32_t index_number = max - rewrite_buffer.num - 1;
					struct connect_info* info = g_hash_table_lookup(for_connect, &index_number);
					if (info != NULL) {
						c->pre = (int16_t)info->pre;
						c->next = (int16_t)info->next;
					}
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}
		//===========注意是否将top哈希表中的所有元素内存释放掉
		g_hash_table_remove_all(top);
		g_hash_table_remove_all(for_connect);
		//删除sequence所有元素
		g_sequence_remove_range(g_sequence_get_begin_iter(rewrite_buffer.container_record_seq),
			g_sequence_get_end_iter(rewrite_buffer.container_record_seq));

	}

	//用来显示区域各容器第一次出现的顺序
	printf("===========once stop analyse area!!!!!!\n");

	//处理最后一段不能填满rewrite_buffer的所有块
	spatial_locality_cap_segment_get_top_fix();
	
	printf("connect_info hashtable information is :\n");
	GList* listValues = g_hash_table_get_values(for_connect);
	GList* start = g_list_first(listValues);
	struct connect_info* info = NULL;
	while(start != NULL ){
		info = start->data;
		printf("info.index_number = %d   //", info->index_number);
		printf("info.pre = %d   //", info->pre);
		printf("info.next = %d   // \n", info->next);
		start = start->next;
	}
	
	
	struct chunk *c;
	//记录rewrite_buffer.num最大值
	int max = rewrite_buffer.num;
	while ((c = rewrite_buffer_popForCapping())) {
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
			&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
			&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			if (g_hash_table_lookup(top, &c->physical_address_start) == NULL) {
				/* not in TOP */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
					chunk_num, c->id);
			}
			else {
				int32_t index_number = max - rewrite_buffer.num - 1;
				struct connect_info* info = g_hash_table_lookup(for_connect, &index_number);
				if (info != NULL) {
					c->pre = (int16_t)info->pre;
					c->next = (int16_t)info->next;
				}
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);
	g_hash_table_remove_all(for_connect);
	//删除sequence所有元素
	g_sequence_remove_range(g_sequence_get_begin_iter(rewrite_buffer.container_record_seq),
		g_sequence_get_end_iter(rewrite_buffer.container_record_seq));

	sync_queue_term(rewrite_queue);

	return NULL;
}


/*
原版capping函数，该函数为destor的修正版
*/
void* cap_rewrite_fix(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
	
	//for LBW
	int cap_num = 6;int cap_allnum = 0;
	int rewrite_cnum = 150; int rewrite_c_allnum = 0;
	int time = 0;
	int rc_read = 0;int rc_write = 0;
	double tc = -1;

	while (1) {
		//查看队头元素
		struct chunk *c = sync_queue_get_top(dedup_queue);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		//printf("ready go to rewrite_buffer_pushForCapping()\n");
		if (!rewrite_buffer_pushForCapping(c)) {
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_pop(dedup_queue);
			continue;
		}
		//printf("ready go to get_top_fix()\n");
		
		//得到满足阈值的所有容器id
		cap_segment_get_top_fix();

		//用来显示容器引用计数分布
		int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
		GSequenceIter *iter = g_sequence_get_begin_iter(
		rewrite_buffer.container_record_seq);
		int max = -1;int min = 999999;float sum = 0.0;
		int i;
		time++;
		//printf("\nthe %dth container sort is :\n", time);
		for(i = 0; i < length; i++){
			assert(!g_sequence_iter_is_end(iter));
			struct containerRecordForCapping* record = g_sequence_get(iter);
			if(i == cap_num * time - cap_allnum -1){
				rc_read = record->chunk_number;
			}
			//printf("%d   ", record->chunk_number);
			if(record->chunk_number > max)
				max = record->chunk_number;
			if(record->chunk_number < min)
				min = record->chunk_number;
			sum += record->chunk_number;
			iter = g_sequence_iter_next(iter);
		}
		/*printf("\n");
		printf("the max count is %d, ", max);
		printf("the min count is %d, ", min);
		printf("the avg of count is %.2f \n", sum/length);*/
		
		if (i < cap_num * time - cap_allnum) {
			rc_read = 0;
		}
		int temp = 0;
		for (i = 0; i < length; i++) {
			iter = g_sequence_iter_prev(iter);
			struct containerRecordForCapping* record = g_sequence_get(iter);
			temp += record->chunk_number;
			rc_write = record->chunk_number;
			if (temp + record->chunk_number > rewrite_cnum * time - rewrite_c_allnum) {
				break;
			}
		}

		{
			if (rc_read > rc_write) {
				tc = rc_write;
			}
			else if (tc > rc_read && tc < rc_write) {
				//tc = (rc_read + rc_write) / 2;
			}
			else {
				tc = (rc_read + rc_write) / 2;
			}

		}
		//printf(" this segment container will be \n");
		iter = g_sequence_get_begin_iter(rewrite_buffer.container_record_seq);
		for (i = 0; i < length; i++) {
			assert(!g_sequence_iter_is_end(iter));
			struct containerRecordForCapping* record = g_sequence_get(iter);
			if (record->chunk_number >= tc) {
				//printf("dedup: %d  ", record->chunk_number);
				cap_allnum++;
			}
			else {
				//printf("rewrite: %d  ", record->chunk_number);
				rewrite_c_allnum += record->chunk_number;
			}
			iter = g_sequence_iter_next(iter);
		}

		//printf("\n TC is %.2f , remain_number_cap is %d , remain_number_rewrite_chunck is %d \n\n",
		//	tc, cap_num * time - cap_allnum, rewrite_cnum * time - rewrite_c_allnum);
		
		
		//用来显示区域各容器第一次出现的顺序
		//printf("===========once stop analyse area!!!!!!\n");


		while ((c = rewrite_buffer_popForCapping())) {
			if (!CHECK_CHUNK(c, CHUNK_FILE_START)
				&& !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
				&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->physical_address_start) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}

		g_hash_table_remove_all(top);
		//删除sequence所有元素
		g_sequence_remove_range(g_sequence_get_begin_iter(rewrite_buffer.container_record_seq),
			g_sequence_get_end_iter(rewrite_buffer.container_record_seq));

	}

	//处理最后一段不能填满rewrite_buffer的所有块
	cap_segment_get_top_fix();
	
	//用来显示容器引用计数分布
	//printf("container sort is :\n");
	int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
	GSequenceIter *iter = g_sequence_get_begin_iter(
	rewrite_buffer.container_record_seq);
	int max = -1;int min = 999999;float sum = 0.0;
	for(int i = 0; i < length; i++){
		assert(!g_sequence_iter_is_end(iter));
		struct containerRecordForCapping* record = g_sequence_get(iter);
		//printf("%d   ", record->chunk_number);
		if(record->chunk_number > max)
			max = record->chunk_number;
		if(record->chunk_number < min)
			min = record->chunk_number;
		sum += record->chunk_number;
		iter = g_sequence_iter_next(iter);
	}
	/*printf("\n");
	printf("the max count is %d, ", max);
	printf("the min count is %d, ", min);
	printf("the avg count is %.2f \n", sum/length);*/
		
		
	//用来显示区域各容器第一次出现的顺序
	//printf("===========once stop analyse area!!!!!!\n");
		
	struct chunk *c;
	while ((c = rewrite_buffer_popForCapping())) {
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
			&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
			&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			if (g_hash_table_lookup(top, &c->physical_address_start) == NULL) {
				/* not in TOP */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
					chunk_num, c->id);
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);
	//删除sequence所有元素
	g_sequence_remove_range(g_sequence_get_begin_iter(rewrite_buffer.container_record_seq),
		g_sequence_get_end_iter(rewrite_buffer.container_record_seq));

	sync_queue_term(rewrite_queue);

	return NULL;
}


/*
 * We first assemble a fixed-sized buffer of pending chunks.
 * Then, counting container utilization in the buffer and sorting.
 * The pending chunks in containers of most low utilization are fragmentation.
 * The main drawback of capping,
 * is that capping overlook the relationship of consecutive buffers.
 */
void *cap_rewrite(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);

	while (1) {
		struct chunk *c = sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_push(c)) {
			TIMER_END(1, jcr.rewrite_time);
			continue;
		}

		//用来显示区域各容器第一次出现的顺序
		printf("===========once stop analyse area!!!!!!\n");

		//得到满足阈值的所有容器id
		cap_segment_get_top();

		while ((c = rewrite_buffer_pop())) {
			if (!CHECK_CHUNK(c,	CHUNK_FILE_START) 
					&& !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}

		g_hash_table_remove_all(top);

	}

	//用来显示区域各容器第一次出现的顺序
	printf("===========once stop analyse area!!!!!!\n");

	//处理最后一段不能填满rewrite_buffer的所有块
	cap_segment_get_top();

	struct chunk *c;
	while ((c = rewrite_buffer_pop())) {
		if (!CHECK_CHUNK(c,	CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			if (g_hash_table_lookup(top, &c->id) == NULL) {
				/* not in TOP */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);

	sync_queue_term(rewrite_queue);

	return NULL;
}




gint g_trigger_chunk_entity_cmp_by_chunkNumber(struct trigger_chunk_entity* a,
	struct trigger_chunk_entity* b, gpointer user_data) {
	//根据块数量倒序排
	int32_t num = b->number - a->number;
	return num == 0 ? 1 : num;	//可以存放chunk_number相同的元素
}
/*
void check_trigger_hash_table(gpointer key, gpointer value, gpointer user_data){
	struct trigger_chunk_entity* trigger = value;
	int *tc = user_data;
	if(trigger->number >= *tc && trigger->role == 1){
		//达到阈值且为重写块集,则将其设置为逃逸块集
		trigger->role = 2;
		rewrite_buffer_my.size -= trigger->size;
		//将队列中所有块角色设置为逃逸块
		int i = 2;
		g_queue_foreach(trigger->chunk_queue, set_chunk_role, &i);
	}
	while(trigger->next != NULL){
		trigger = trigger->next ;
		if(trigger->number >= *tc && trigger->role == 1){
			//达到阈值且为重写块集,则将其设置为逃逸块集
			trigger->role = 2;
			rewrite_buffer_my.size -= trigger->size;
			//将队列中所有块角色设置为逃逸块
			int i = 2;
			g_queue_foreach(trigger->chunk_queue, set_chunk_role, &i);
		}
	}
}
*/


//调整tc函数
int adjust_tc_my(int *n_cap, int *n_rewrite, int every_allow_cap, int every_allow_rewrite,
		int tc, GHashTable * trigger_chunk_table) {
	static int time_sort = 0;
	int rc_read = 100000;
	int rc_rewrite = 0;
	
	int i;
	GSequence* for_sort = g_sequence_new(free_trigger_chunk_entity);
	GList* listValues = g_hash_table_get_values(trigger_chunk_table);
	GList* start = g_list_first(listValues);
	struct trigger_chunk_entity* info = NULL;
	while (start != NULL) {
		info = start->data;
		if (info->byte_position < rewrite_buffer_my.mainArea_headPositon) {
			struct trigger_chunk_entity* entity = new_trigger_chunk_entity(info->id, 
				info->byte_position);
			entity->number = info->number;
			g_sequence_insert_sorted(for_sort, entity, g_trigger_chunk_entity_cmp_by_chunkNumber, NULL);
		}
		while (info->next) {
			info = info->next;
			if (info->byte_position < rewrite_buffer_my.mainArea_headPositon) {
				struct trigger_chunk_entity* entity = new_trigger_chunk_entity(info->id, 
					info->byte_position);
				entity->number = info->number;
				g_sequence_insert_sorted(for_sort,
					entity, g_trigger_chunk_entity_cmp_by_chunkNumber, NULL);
			}
		}
		start = start->next;
	}
	//用来显示容器引用计数分布
	time_sort++;
	printf("\nthe %dth adjust, the containersort is :\n", time_sort);
	int length = g_sequence_get_length(for_sort);
	printf("the for_srot length is %d\n", length);
	//序列为空时
	if(length == 0){
		return tc;
	}
	
	int max = -1; int min = 999999; float sum = 0.0;
	int rc_read_sort = time_sort * every_allow_cap - *n_cap;
	int rc_rewrite_number = time_sort * every_allow_rewrite - *n_rewrite;
	printf("rc_read_sort is %d, rc_rewrite_number is %d\n", rc_read_sort, rc_rewrite_number);
	GSequenceIter *iter = g_sequence_get_begin_iter(for_sort);
	for (i = 0; i < length; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct trigger_chunk_entity* record = g_sequence_get(iter);
		if (i == rc_read_sort - 1) {
			rc_read = record->number;
		}
		printf("%d   ", record->number);
		if (record->number > max)
			max = record->number;
		if (record->number < min)
			min = record->number;
		sum += record->number;
		iter = g_sequence_iter_next(iter);
	}
	printf("\n");
	printf("the max count is %d, ", max);
	printf("the min count is %d, ", min);
	printf("the avg of count is %.2f \n", sum / length);

	if (i < rc_read_sort) {
		rc_read = 0;
	}
	printf("rc_read is %d, ",rc_read);
	int temp = 0;
	for (i = 0; i < length; i++) {
		iter = g_sequence_iter_prev(iter);
		struct trigger_chunk_entity* record = g_sequence_get(iter);
		temp += record->number;
		rc_rewrite = record->number;
		if (temp > rc_rewrite_number) {
			break;
		}
	}
	printf("rc_rewrite is %d\n",rc_rewrite);
	printf("the last TC is %d, last segement used %d cap , last segement used %d rewrite_chunck\n",
		tc, *n_cap , *n_rewrite);
	
	{
		int temp_tc = 0;
		if (rc_read > rc_rewrite) {
			temp_tc = rc_rewrite;
		}
		else if (tc > rc_read && tc < rc_rewrite) {
			//tc = (rc_read + rc_rewrite) / 2;
			temp_tc = tc;
		}
		else {
			temp_tc = (rc_read + rc_rewrite) / 2;
		}
		tc = temp_tc;
		//强制稳定措施
		/*
		if (temp_tc < (int)(tc * 0.9)) {
			tc = rc_rewrite;
			printf("####warning: mandatory to adjust TC, the former TC is %d, now TC is %d ####\n",
				temp_tc, tc);
		}else{
			tc = temp_tc;
		}
		*/

	}
	printf(" this segment container will be \n");
	iter = g_sequence_get_begin_iter(for_sort);
	for (i = 0; i < length; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct trigger_chunk_entity* record = g_sequence_get(iter);
		if (record->number >= tc) {
			printf("dedup: %d  ", record->number);
			//删除最老的触发块
			{
				//先拿到最老的触发块记录
				struct trigger_chunk_entity* trigger =
					g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &record->id);
				assert(trigger != NULL);
				assert(rewrite_buffer_my.mainArea_headPositon > trigger->byte_position);
				struct trigger_chunk_entity* new_trigger = trigger->next;
				(*n_cap)++;
				if (new_trigger == NULL) {
					g_hash_table_remove(rewrite_buffer_my.trigger_chunk_table, &record->id);
				}
				else {
					g_hash_table_replace(rewrite_buffer_my.trigger_chunk_table,
						&new_trigger->id, new_trigger);
				}
				//printf("=====use a cap and remove a entity from trigger_hash_table, the n_cap is %d\n, the byte position is %ld\n", *n_cap, record->byte_position);
			}
		}
		else {
			printf("rewrite: %d  ", record->number);
			//删除最老的触发块
			{
				//先拿到最老的触发块记录
				struct trigger_chunk_entity* trigger =
					g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &record->id);
				assert(trigger != NULL);
				assert(rewrite_buffer_my.mainArea_headPositon > trigger->byte_position);
				struct trigger_chunk_entity* new_trigger = trigger->next;
				(*n_rewrite) += trigger->number;
				//设置块标记为重写
				g_queue_foreach(trigger->chunk_queue, set_chunk_outoforder, NULL);

				if (new_trigger == NULL) {
					g_hash_table_remove(rewrite_buffer_my.trigger_chunk_table, &record->id);
				}
				else {
					g_hash_table_replace(rewrite_buffer_my.trigger_chunk_table,
						&new_trigger->id, new_trigger);
				}
				//printf("=====use a cap and remove a entity from trigger_hash_table, the n_cap is %d\n, the byte position is %ld\n", *n_cap, record->byte_position);
			}

		}
		iter = g_sequence_iter_next(iter);
	}
	printf("\n TC is %d , this segement can use %d cap , this segement can use %d rewrite_chunck  \n",
		tc, rc_read_sort , rc_rewrite_number);

	g_sequence_free(for_sort);

	return tc;
}


/*
该函数为自己想法的函数
*/
void* cap_rewrite_doubleSize(void* arg) {
	init_rewrite_buffer_my();

	int tc = 100000;
	int n_cap = 0;
	int n_rewrite = 0;
	int every_allow_cap = destor.rewrite_algorithm[3];
	int every_allow_rewrite = destor.rewrite_algorithm[2];
	
	//先将主区域塞满
	while (1) {
		//查看队头元素
		struct chunk *c = sync_queue_get_top(dedup_queue);
		if (c == NULL) {
			printf("main area can't full !!!!");
			exit(0);
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_pushForMy(c, rewrite_buffer_my.chunk_queue)) {
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_pop(dedup_queue);
			continue;
		}
		break;
	}
	rewrite_buffer_my.look_ahead_tailPositon = rewrite_buffer_my.mainArea_headPositon;
	rewrite_buffer_my.look_ahead_headPositon = rewrite_buffer_my.mainArea_headPositon;

	while (1) {
		//查看队头元素
		struct chunk *c = sync_queue_get_top(dedup_queue);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_pushForMy(c, rewrite_buffer_my.look_ahead_chunk_queue)) {
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_pop(dedup_queue);
			continue;
		}
		tc = adjust_tc_my(&n_cap, &n_rewrite, every_allow_cap, every_allow_rewrite, tc,
			rewrite_buffer_my.trigger_chunk_table);
		printf("at mainArea_tailPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_tailPositon);
		printf("at mainArea_headPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_headPositon);
		double i = (rewrite_buffer_my.mainArea_headPositon -
			rewrite_buffer_my.mainArea_tailPositon) / 1024.0 / 1024.0;
		printf("size of mainarea is %.2fMB \n", i);

		//将主区域数据块全部弹出发送至下一步
		while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)) {
			sync_queue_push(rewrite_queue, c);
		}

		//将超前区数据块全部转入主区域
		while (c = g_queue_pop_head(rewrite_buffer_my.look_ahead_chunk_queue)) {
			g_queue_push_tail(rewrite_buffer_my.chunk_queue, c);
		}
		rewrite_buffer_my.size = rewrite_buffer_my.look_ahead_size;
		rewrite_buffer_my.mainArea_tailPositon = rewrite_buffer_my.look_ahead_tailPositon;
		rewrite_buffer_my.mainArea_headPositon = rewrite_buffer_my.look_ahead_headPositon;

		rewrite_buffer_my.look_ahead_tailPositon = rewrite_buffer_my.mainArea_headPositon;
		rewrite_buffer_my.look_ahead_headPositon = rewrite_buffer_my.mainArea_headPositon;
		rewrite_buffer_my.look_ahead_size = 0;
	}

	//最后超前去未满时
	tc = adjust_tc_my(&n_cap, &n_rewrite, every_allow_cap, every_allow_rewrite, tc,
		rewrite_buffer_my.trigger_chunk_table);
	printf("at mainArea_tailPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_tailPositon);
	printf("at mainArea_headPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_headPositon);
	double i = (rewrite_buffer_my.mainArea_headPositon -
		rewrite_buffer_my.mainArea_tailPositon) / 1024.0 / 1024.0;
	printf("size of mainarea is %.2fMB \n", i);

	//将主区域数据块全部弹出发送至下一步
	struct chunk* c;
	while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)) {
		sync_queue_push(rewrite_queue, c);
	}

	//将超前区数据块全部转入主区域
	while (c = g_queue_pop_head(rewrite_buffer_my.look_ahead_chunk_queue)) {
		g_queue_push_tail(rewrite_buffer_my.chunk_queue, c);
	}
	rewrite_buffer_my.mainArea_tailPositon = rewrite_buffer_my.look_ahead_tailPositon;
	rewrite_buffer_my.mainArea_headPositon = rewrite_buffer_my.look_ahead_headPositon;

	rewrite_buffer_my.look_ahead_tailPositon = rewrite_buffer_my.mainArea_headPositon;
	rewrite_buffer_my.look_ahead_headPositon = rewrite_buffer_my.mainArea_headPositon;

	int length = g_queue_get_length(rewrite_buffer_my.chunk_queue);
	if (length > 0) {
		tc = adjust_tc_my(&n_cap, &n_rewrite, every_allow_cap, every_allow_rewrite, tc,
			rewrite_buffer_my.trigger_chunk_table);
		printf("at mainArea_tailPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_tailPositon);
		printf("at mainArea_headPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_headPositon);
		double i = (rewrite_buffer_my.mainArea_headPositon -
			rewrite_buffer_my.mainArea_tailPositon) / 1024.0 / 1024.0;
		printf("size of mainarea is %.2fMB \n", i);

		//将主区域数据块全部弹出发送至下一步
		struct chunk* c;
		while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)) {
			sync_queue_push(rewrite_queue, c);
		}
	}

	printf("\n========end, total use cap number is %d, total rewrite chunk number is %d =======\n",
		n_cap, n_rewrite);
	sync_queue_term(rewrite_queue);

	return NULL;
}


/*
该函数为FCRC的函数
*/
void* cap_rewrite_fcrc(void* arg) {
	init_rewrite_buffer_my();

	int tc = 100000;
	int n_cap = 0;
	int n_rewrite = 0;
	int every_allow_cap = destor.rewrite_algorithm[3];
	int every_allow_rewrite = destor.rewrite_algorithm[2];

	while (1) {
		//查看队头元素
		struct chunk *c = sync_queue_get_top(dedup_queue);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_pushForMy(c, rewrite_buffer_my.chunk_queue)) {
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_pop(dedup_queue);
			continue;
		}

		tc = adjust_tc_my(&n_cap, &n_rewrite, every_allow_cap, every_allow_rewrite, tc,
			rewrite_buffer_my.trigger_chunk_table);
		printf("at mainArea_tailPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_tailPositon);
		printf("at mainArea_headPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_headPositon);
		double i = (rewrite_buffer_my.mainArea_headPositon -
			rewrite_buffer_my.mainArea_tailPositon) / 1024.0 / 1024.0;
		printf("size of mainarea is %.2fMB \n", i);

		//将主区域数据块全部弹出发送至下一步
		while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)) {
			sync_queue_push(rewrite_queue, c);
		}

		rewrite_buffer_my.mainArea_tailPositon = rewrite_buffer_my.mainArea_headPositon;
		rewrite_buffer_my.size = 0;
	}

	//最后未填满的一段
	int length = g_queue_get_length(rewrite_buffer_my.chunk_queue);
	if (length > 0) {
		tc = adjust_tc_my(&n_cap, &n_rewrite, every_allow_cap, every_allow_rewrite, tc,
			rewrite_buffer_my.trigger_chunk_table);
		printf("at mainArea_tailPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_tailPositon);
		printf("at mainArea_headPositon byte %ld to adjust tc\n", rewrite_buffer_my.mainArea_headPositon);
		double i = (rewrite_buffer_my.mainArea_headPositon -
			rewrite_buffer_my.mainArea_tailPositon) / 1024.0 / 1024.0;
		printf("size of mainarea is %.2fMB \n", i);

		//将主区域数据块全部弹出发送至下一步
		struct chunk* c;
		while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)) {
			sync_queue_push(rewrite_queue, c);
		}
		rewrite_buffer_my.mainArea_tailPositon = rewrite_buffer_my.mainArea_headPositon;
		rewrite_buffer_my.size = 0;
	}

	printf("\n========end, total use cap number is %d, total rewrite chunk number is %d =======\n",
		n_cap, n_rewrite);
		printf("rewrite_buffer_my.set_chunk_outOfOrder_count is %d\n\n",rewrite_buffer_my.set_chunk_outOfOrder_count);	
	
	sync_queue_term(rewrite_queue);

	return NULL;
}


//原LBW的调整tc函数
int adjust_tc(int *n_cap, int *n_rewrite, int tc, int every_allow_cap, int every_allow_rewrite,
		GHashTable * trigger_chunk_table) {
	static int time_sort = 0;
	int rc_read = 100000;
	int rc_rewrite = 0;

	int i;
	GSequence* for_sort = g_sequence_new(NULL);
	GList* listValues = g_hash_table_get_values(trigger_chunk_table);
	GList* start = g_list_first(listValues);
	struct trigger_chunk_entity* info = NULL;
	while (start != NULL) {
		info = start->data;
		g_sequence_insert_sorted(for_sort, info, g_trigger_chunk_entity_cmp_by_chunkNumber, NULL);
		start = start->next;
	}

	//用来显示容器引用计数分布
	time_sort++;
	printf("\nthe %dth adjust, the containersort is :\n", time_sort);
	int length = g_sequence_get_length(for_sort);
	printf("the for_srot length is %d\n", length);
	if (length == 0) {
		return tc;
	}
	int max = -1; int min = 999999; float sum = 0.0;
	int rc_read_sort = time_sort * every_allow_cap - *n_cap;
	int rc_rewrite_number = time_sort * every_allow_rewrite - *n_rewrite;
	printf("rc_read_sort is %d, rc_rewrite_number is %d\n", rc_read_sort, rc_rewrite_number);
	GSequenceIter *iter = g_sequence_get_begin_iter(for_sort);
	for (i = 0; i < length; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct trigger_chunk_entity* record = g_sequence_get(iter);
		if (i == rc_read_sort - 1) {
			rc_read = record->number;
		}
		printf("%d   ", record->number);
		if (record->number > max)
			max = record->number;
		if (record->number < min)
			min = record->number;
		sum += record->number;
		iter = g_sequence_iter_next(iter);
	}
	printf("\n");
	printf("the max count is %d, ", max);
	printf("the min count is %d, ", min);
	printf("the avg of count is %.2f \n", sum / length);

	if (i < rc_read_sort) {
		rc_read = 0;
	}
	printf("rc_read is %d, ", rc_read);
	int temp = 0;
	for (i = 0; i < length; i++) {
		iter = g_sequence_iter_prev(iter);
		struct trigger_chunk_entity* record = g_sequence_get(iter);
		temp += record->number;
		rc_rewrite = record->number;
		if (temp > rc_rewrite_number) {
			break;
		}
	}
	printf("rc_rewrite is %d\n", rc_rewrite);
	printf("the last TC is %d, last segement used %d cap , last segement used %d rewrite_chunck\n",
		tc, *n_cap, *n_rewrite);

	{
		if (rc_read > rc_rewrite) {
			tc = rc_rewrite;
		}
		else if (tc > rc_read && tc < rc_rewrite) {
			//tc = (rc_read + rc_rewrite) / 2;
		}
		else {
			tc = (rc_read + rc_rewrite) / 2;
		}

	}
	printf(" this segment container will be \n");
	iter = g_sequence_get_begin_iter(for_sort);
	for (i = 0; i < length; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct trigger_chunk_entity* record = g_sequence_get(iter);
		if (record->number >= tc) {
			printf("dedup: %d  ", record->number);
			(*n_cap)++;
		}
		else {
			printf("maybe rewrite: %d  ", record->number);
			(*n_rewrite) += record->number;
		}
		iter = g_sequence_iter_next(iter);
	}
	printf("\n TC is %d , this segement can use %d cap , this segement can use %d rewrite_chunck  \n",
		tc, rc_read_sort, rc_rewrite_number);

	g_sequence_free(for_sort);

	return tc;
}

/*
该函数为原LBW
*/
/*
void* cap_rewrite_lbw(void* arg) {
	init_rewrite_buffer_my();

	//for LBW
	int tc = destor.rewrite_algorithm[4];
	int n_cap = 0;
	int n_rewrite = 0;
	int every_allow_cap = destor.rewrite_algorithm[3];
	int every_allow_rewrite = destor.rewrite_algorithm[2];

	int64_t last_cycle_position = -(destor.restore_cache[1] - 1) * 4 * 1024 * 1024;


	GQueue *queue_for_cycle = g_queue_new();
	//int64_t *cycle_start = malloc(sizeof(int64_t));
	//*cycle_start = 0;
	//g_queue_push_tail(queue_for_cycle, cycle_start);
	

	while (1) {
		//查看队头元素
		struct chunk *c = sync_queue_get_top(dedup_queue);
		if (c == NULL)
			break;


		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		//printf("ready go to rewrite_buffer_pushForLBW()\n");
		if (!rewrite_buffer_pushForLBW(c, tc)) {
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_pop(dedup_queue);
			continue;
		}

		if (rewrite_buffer_my.tail_position - last_cycle_position >= 
				(destor.restore_cache[1] - 1) * 4 * 1024 * 1024) {
			last_cycle_position = rewrite_buffer_my.tail_position;
			tc = adjust_tc(&n_cap, &n_rewrite, tc, every_allow_cap, every_allow_rewrite,
				rewrite_buffer_my.trigger_chunk_table);
			printf("at tail_lbw byte %ld to adjust tc\n", rewrite_buffer_my.tail_position);
			printf("at head_lbw byte %ld to adjust tc\n", rewrite_buffer_my.head_position);
			double i = (rewrite_buffer_my.head_position -
				rewrite_buffer_my.tail_position) / 1024.0 / 1024.0;
			printf("size of LBW is %.2fMB \n\n", i);
		}
		rewrite_buffer_popForLBW();

	}

	printf("process last segement!!\n");
	struct chunk* c;
	while (c = rewrite_buffer_popForLBW()) {

	}

	printf("\n========end, total use cap number is %d, total rewrite chunk number is %d =======\n",
		n_cap, n_rewrite);
	sync_queue_term(rewrite_queue);

	return NULL;
}
*/

