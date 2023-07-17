/*
 * In the phase,
 * we mark the chunks required to be rewriting.
 */
#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"

static pthread_t rewrite_t;

/* Descending order */
gint g_record_descmp_by_length(struct containerRecord* a,
		struct containerRecord* b, gpointer user_data) {
	//如果第一个出现在第二个之前，则返回负值
	return b->size - a->size;
}

glong g_record_cmp_by_id(struct containerRecord* a, struct containerRecord* b,
		gpointer user_data) {
	return a->cid - b->cid;
}

glong g_recordForCapping_cmp_by_startAddress(struct containerRecordForCapping* a,
		struct containerRecordForCapping* b, gpointer user_data) {
	return  a->physical_address_start - b->physical_address_start;
}
gint g_recordForCapping_cmp_by_chunkNumber(struct containerRecordForCapping* a,
	struct containerRecordForCapping* b, gpointer user_data) {
	//根据块数量倒序排
	int32_t num = b->chunk_number - a->chunk_number;
	return num == 0 ? 1 : num;	//可以存放chunk_number相同的元素
}

gint g_localityContainerRecord_cmp_by_number(struct localityContainerRecord *a,
		struct localityContainerRecord *b, gpointer user_data) {
	//根据块数量倒序排
	int32_t num = b->chunk_number - a->chunk_number;
	return num == 0 ? 1 : num;	//可以存放chunk_number相同的元素
	
}

static void init_rewrite_buffer() {
	rewrite_buffer.chunk_queue = g_queue_new();
	rewrite_buffer.container_record_seq = g_sequence_new(free);
	rewrite_buffer.num = 0;
	rewrite_buffer.size = 0;
}

//初始化
void init_rewrite_buffer_my() {
	rewrite_buffer_my.chunk_queue = g_queue_new();
	rewrite_buffer_my.look_ahead_chunk_queue = g_queue_new();
	rewrite_buffer_my.trigger_chunk_table =
		g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free_trigger_chunk_entity);
	rewrite_buffer_my.size = 0;
	rewrite_buffer_my.look_ahead_size = 0;
	rewrite_buffer_my.look_ahead_size = 0;
	rewrite_buffer_my.mainArea_tailPositon = 0;
	rewrite_buffer_my.mainArea_headPositon = 0;
	rewrite_buffer_my.look_ahead_tailPositon = 0;
	rewrite_buffer_my.look_ahead_headPositon = 0;
	rewrite_buffer_my.set_chunk_outOfOrder_count = 0;
}
struct trigger_chunk_entity*  new_trigger_chunk_entity(int64_t id, int64_t byte_position) {
	struct trigger_chunk_entity* trigger = malloc(sizeof(struct trigger_chunk_entity));
	trigger->id = id;
	//printf("===new a trigger_entity ,it's byte position is %ld\n",byte_position);
	trigger->byte_position = byte_position;
	trigger->number = 0;
	trigger->size = 0;
	trigger->chunk_queue = g_queue_new();
	trigger->next = NULL;
	return trigger;
}
void free_trigger_chunk_entity(gpointer data) {
	struct trigger_chunk_entity* p = data;
	g_queue_free(p->chunk_queue);
	p->chunk_queue = NULL;
	free(p);
}
void set_chunk_outoforder(gpointer data, gpointer userdata) {
	struct chunk *c = (struct chunk*)data;
	SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
	rewrite_buffer_my.set_chunk_outOfOrder_count++;
}

/*
return 0 代表队列未满
return 1 代表队列满了
为了LBW改进版,将块放入逻辑超前区
*/
/*
int rewrite_buffer_pushTo_lookAhead(struct chunk* c) {
	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
		|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		g_queue_push_tail(rewrite_buffer_my.look_ahead_chunk_queue, c);
		return 0;
	}

	if (c->size + rewrite_buffer_my.look_ahead_size + rewrite_buffer_my.size 
			> destor.rewrite_algorithm[1]) {
		return 1;
	}

	g_queue_push_tail(rewrite_buffer_my.look_ahead_chunk_queue, c);

	if (c->id != TEMPORARY_ID) {
		assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
		gboolean flag = g_hash_table_contains(rewrite_buffer_my.allArea_trigger_chunk_table, &c->id);
		if (flag == FALSE) {
			//当前LBW的超前触发表内没有该块容器ID记录，则该块被视为“自然触发块"
			struct trigger_chunk_entity* trigger =
				new_trigger_chunk_entity(c->id, rewrite_buffer_my.look_ahead_positon);
			g_queue_push_tail(trigger->chunk_queue, c);
			trigger->number++;
			g_hash_table_insert(rewrite_buffer_my.allArea_trigger_chunk_table, &trigger->id, trigger);
		}
		else {
			struct trigger_chunk_entity* trigger =
				g_hash_table_lookup(rewrite_buffer_my.allArea_trigger_chunk_table, &c->id);
			//trigger指向最新的触发块记录
			while (trigger->next != NULL) {
				trigger = trigger->next;
			}
			if (rewrite_buffer_my.look_ahead_positon + c->size - trigger->byte_position
					> (destor.restore_cache[1] - 1) * 4 * 1024 * 1024) {
				//超出默认距离
				struct trigger_chunk_entity* trigger_end =
					new_trigger_chunk_entity(c->id, rewrite_buffer_my.look_ahead_positon);
				g_queue_push_tail(trigger_end->chunk_queue, c);
				trigger_end->number++;
				//将被迫触发的块构成的实体放入同ID链的最后
				trigger->next = trigger_end;
			}
			else {
				//未超出默认距离
				g_queue_push_tail(trigger->chunk_queue, c);
				trigger->number++;
			}
		}
	}
	else {
		//为唯一块，啥也不做
	}

	rewrite_buffer_my.look_ahead_positon += c->size;
	//所有超前区域的块全部放入缓存区域
	rewrite_buffer_my.look_ahead_size += c->size;

	return 0;
}
*/

/*
return 0 代表队列未满
return 1 代表队列满了
为了自己的想法
*/
int rewrite_buffer_pushForMy(struct chunk* c, GQueue *queue) {
	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
		|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		g_queue_push_tail(queue, c);
		return 0;
	}

	int64_t *area_size = NULL;
	int64_t *area_headPositon = NULL;
	if (queue == rewrite_buffer_my.chunk_queue) {
		area_size = &(rewrite_buffer_my.size);
		area_headPositon = &(rewrite_buffer_my.mainArea_headPositon);
	}
	if (queue == rewrite_buffer_my.look_ahead_chunk_queue) {
		area_size = &(rewrite_buffer_my.look_ahead_size);
		area_headPositon = &(rewrite_buffer_my.look_ahead_headPositon);
	}


	if (c->size + *area_size > destor.rewrite_algorithm[1]) {
		return 1;
	}

	g_queue_push_tail(queue, c);

	if (c->id != TEMPORARY_ID) {
		assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
		gboolean flag = g_hash_table_contains(rewrite_buffer_my.trigger_chunk_table, &c->id);
		if (flag == FALSE) {
			//当前触发表内没有该块容器ID记录，则该块被视为“自然触发块"
			struct trigger_chunk_entity* trigger =
				new_trigger_chunk_entity(c->id, *area_headPositon);
			g_queue_push_tail(trigger->chunk_queue, c);
			trigger->number++;
			g_hash_table_insert(rewrite_buffer_my.trigger_chunk_table, &trigger->id, trigger);
			trigger->size += c->size;
		}
		else {
			struct trigger_chunk_entity* trigger = 
				g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &c->id);
			//trigger指向最新的触发块记录
			while (trigger->next != NULL) {
				trigger = trigger->next;
			}
			if ( *area_headPositon + c->size - trigger->byte_position
					> destor.rewrite_algorithm[1] ){
				//超出默认距离
				struct trigger_chunk_entity* trigger_end =
					new_trigger_chunk_entity(c->id, *area_headPositon);
				g_queue_push_tail(trigger_end->chunk_queue, c);
				trigger_end->number++;
				trigger_end->size += c->size;
				//将被迫触发的块构成的实体放入同ID链的最后
				trigger->next = trigger_end;
			}
			else {
				//未超出默认距离
				g_queue_push_tail(trigger->chunk_queue, c);
				trigger->number++;
				trigger->size += c->size;
			}
		}
	}

	(*area_size) += c->size;
	(*area_headPositon) += c->size;
	return 0;
}
/*
struct chunk* rewrite_buffer_popForMy(int *n_cap, int *n_rewrite) {
	struct chunk* c = NULL;
	while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)){
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			sync_queue_push(rewrite_queue, c);
			continue;
		}
		break;
	}

	if (c == NULL) {
		return NULL;
	}
	//assert(rewrite_buffer_my.tail_position < 4000000);
	//printf("===will to check chunk role on byte_position is %ld \n",rewrite_buffer_my.tail_position);
	//printf("c->role is %d\n",c->role);
	if (c->role == 2) {
		//printf("when c->role is 2 ===\n");
		//该块为重写逃逸块，先拿到最老的触发块记录
		struct trigger_chunk_entity* trigger =
			g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &c->id);
		if (trigger != NULL && rewrite_buffer_my.tail_position == trigger->byte_position) {
			//如果该重写逃逸块字节位置等于查询出来的字节位置，则删除该项记录
			struct trigger_chunk_entity* new_trigger = trigger->next;
			(*n_cap)++;
			if (new_trigger == NULL) {
				g_hash_table_remove(rewrite_buffer_my.trigger_chunk_table, &c->id);
			}
			else {
				g_hash_table_replace(rewrite_buffer_my.trigger_chunk_table,
					&new_trigger->id, new_trigger);
			}
			printf("=====use a cap and remove a entity from trigger_hash_table, the n_cap is %d\n, the byte position is %ld\n", *n_cap, rewrite_buffer_my.tail_position);
		}
		if (trigger != NULL && rewrite_buffer_my.tail_position < trigger->byte_position) {
			//如果该重写逃逸块字节位置小于查询出来的字节位置，不做其他特殊操作
		}

		//处理allArea_trigger_chunk_table
		{
			struct trigger_chunk_entity* trigger =
				g_hash_table_lookup(rewrite_buffer_my.allArea_trigger_chunk_table, &c->id);
			if (trigger != NULL && rewrite_buffer_my.tail_position == trigger->byte_position) {
				//如果该重写逃逸块字节位置等于查询出来的字节位置，则删除该项记录
				struct trigger_chunk_entity* new_trigger = trigger->next;
				if (new_trigger == NULL) {
					g_hash_table_remove(rewrite_buffer_my.allArea_trigger_chunk_table, &c->id);
				}
				else {
					g_hash_table_replace(rewrite_buffer_my.allArea_trigger_chunk_table,
						&new_trigger->id, new_trigger);
				}
			}
			if (trigger != NULL && rewrite_buffer_my.tail_position < trigger->byte_position) {
				//如果该重写逃逸块字节位置小于查询出来的字节位置，不做其他特殊操作
			}
		}

	}
	if (c->role == 1) {
		//printf("when c->role is 1 ===\n");
		//该块为重写块，先拿到最老的触发块记录，应该就是自己
		struct trigger_chunk_entity* trigger =
			g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &c->id);
		assert(trigger->id == c->id);
		assert(trigger != NULL);
		//int length = g_queue_get_length(trigger->chunk_queue);
		int i = 10;//设置为最终重写角色
		g_queue_foreach(trigger->chunk_queue, set_chunk_role, &i);
		
		//将确定为最终重写的所有块移除
		rewrite_buffer_my.size -= trigger->size;

		(*n_rewrite) += trigger->number;
		struct trigger_chunk_entity* new_trigger = trigger->next;
		if (new_trigger == NULL) {
			g_hash_table_remove(rewrite_buffer_my.trigger_chunk_table, &c->id);
		}
		else {
			g_hash_table_replace(rewrite_buffer_my.trigger_chunk_table,
				&new_trigger->id, new_trigger);
		}

		//处理allArea_trigger_chunk_table
		{
			struct trigger_chunk_entity* trigger =
				g_hash_table_lookup(rewrite_buffer_my.allArea_trigger_chunk_table, &c->id);
			assert(trigger->id == c->id);
			assert(trigger != NULL);
			//int length = g_queue_get_length(trigger->chunk_queue);
			struct trigger_chunk_entity* new_trigger = trigger->next;
			if (new_trigger == NULL) {
				g_hash_table_remove(rewrite_buffer_my.allArea_trigger_chunk_table, &c->id);
			}
			else {
				g_hash_table_replace(rewrite_buffer_my.allArea_trigger_chunk_table,
					&new_trigger->id, new_trigger);
			}
		}

	}

	if (c->role == 10) {
		SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
	}

	//剩下的就是唯一块，不需要做什么操作
	//发送到下一个步骤
	sync_queue_push(rewrite_queue, c);
	rewrite_buffer_my.num--;
	rewrite_buffer_my.tail_position += c->size;
	return c;
}
*/

/*
return 0 代表队列未满
return 1 代表队列满了
为了原版的LBW
*/
/*
int rewrite_buffer_pushForLBW(struct chunk *c, int tc) {
	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
		|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		g_queue_push_tail(rewrite_buffer_my.chunk_queue, c);
		return 0;
	}

	if (c->size + rewrite_buffer_my.size > destor.rewrite_algorithm[1]) {
		return 1;
	}
	//控制LBW长度
	if (c->size + rewrite_buffer_my.head_position - rewrite_buffer_my.tail_position >=
		(destor.restore_cache[1] - 1) * 4 * 1024 * 1024) {
		return 1;
	}

	g_queue_push_tail(rewrite_buffer_my.chunk_queue, c);

	if (c->id != TEMPORARY_ID) {
		assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
		gboolean flag = g_hash_table_contains(rewrite_buffer_my.trigger_chunk_table, &c->id);
		if (flag == FALSE) {
			//当前LBW的表内没有该块容器ID记录，则该块被视为“前导块"
			struct trigger_chunk_entity* trigger =
				new_trigger_chunk_entity(c->id, rewrite_buffer_my.head_position);
			g_queue_push_tail(trigger->chunk_queue, c);
			trigger->size += c->size;
			trigger->number++;
			trigger->role = 1; //为重写角色
			c->role = 1;
			g_hash_table_insert(rewrite_buffer_my.trigger_chunk_table, &trigger->id, trigger);
			//代表放入重写区域
			rewrite_buffer_my.size += c->size;
		}
		else {
			struct trigger_chunk_entity* trigger =
				g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &c->id);
			if (trigger->role == 2) {
				//前面的块角色为逃逸块
				g_queue_push_tail(trigger->chunk_queue, c);
				trigger->number++;
				trigger->size += c->size;
				c->role = 2;
			}
			else if (trigger->role == 1) {
				//前面的块角色为重写块
				g_queue_push_tail(trigger->chunk_queue, c);
				trigger->number++;
				trigger->size += c->size;
				c->role = 1;
				//代表放入重写区域
				rewrite_buffer_my.size += c->size;
				if (trigger->number >= tc) {
					//代表移除重写区域，并设置为逃逸块集
					trigger->role = 2;
					rewrite_buffer_my.size -= trigger->size;
					//将队列中所有块角色设置为逃逸块
					int i = 2;
					g_queue_foreach(trigger->chunk_queue, set_chunk_role, &i);
				}
			}
		}
	}
	else {
		//为唯一块，啥也不做
	}

	rewrite_buffer_my.num++;
*/
	/*
		int64_t *last_cycle = g_queue_peek_tail(queue_for_cycle);
		if (last_cycle != NULL && rewrite_buffer_my.head_position + c->size - *last_cycle > destor.rewrite_algorithm[1]) {
			int64_t *cycle_start = malloc(sizeof(int64_t));
			*cycle_start = rewrite_buffer_my.head_position;
			g_queue_push_tail(queue_for_cycle, cycle_start);
		}
	*/
/*
	rewrite_buffer_my.head_position += c->size;
	return 0;

}
*/

/*
struct chunk* rewrite_buffer_popForLBW() {
	struct chunk* c = NULL;
	while (c = g_queue_pop_head(rewrite_buffer_my.chunk_queue)) {
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			sync_queue_push(rewrite_queue, c);
			continue;
		}
		break;
	}

	if (c == NULL) {
		return NULL;
	}

	if (c->role == 2) {
		//printf("when c->role is 2 ===\n");
		struct trigger_chunk_entity* trigger =
			g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &c->id);
		assert(trigger !=NULL);
		trigger->number--;
		g_queue_pop_head(trigger->chunk_queue);
		trigger->size -= c->size;
		if (trigger->number <= 0) {
			printf("remove entity from hashtable, the chunk id is %ld\n",c->id);
			g_hash_table_remove(rewrite_buffer_my.trigger_chunk_table, &c->id);
		}
	}
	if (c->role == 1) {
		//printf("when c->role is 1 ===\n");
		struct trigger_chunk_entity* trigger =
			g_hash_table_lookup(rewrite_buffer_my.trigger_chunk_table, &c->id);
		assert(trigger->id == c->id);
		assert(trigger != NULL);
		//int length = g_queue_get_length(trigger->chunk_queue);
		int i = 10;//设置为最终重写角色
		g_queue_foreach(trigger->chunk_queue, set_chunk_role, &i);

		//将确定为最终重写的所有块移除
		rewrite_buffer_my.size -= trigger->size;

		g_hash_table_remove(rewrite_buffer_my.trigger_chunk_table, &c->id);
	}

	if (c->role == 10) {
		SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
	}

	//剩下的就是唯一块，不需要做什么操作
	//发送到下一个步骤
	sync_queue_push(rewrite_queue, c);
	rewrite_buffer_my.tail_position += c->size;
	return c;
}
*/

/*
return 0 代表队列未满
return 1 代表队列满了
*/
int rewrite_buffer_pushForCapping(struct chunk* c) {
	
	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
		|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		g_queue_push_tail(rewrite_buffer.chunk_queue, c);
		return 0;
	}
	else if (c->size + rewrite_buffer.size > destor.rewrite_algorithm[1]) {
		return 1;
	}
	
	g_queue_push_tail(rewrite_buffer.chunk_queue, c);

	if (c->id != TEMPORARY_ID) {
		assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
		struct containerRecordForCapping tmp_record;
		tmp_record.physical_address_start = c->physical_address_start;

		GSequenceIter *iter = g_sequence_lookup(
			rewrite_buffer.container_record_seq, &tmp_record,
			g_recordForCapping_cmp_by_startAddress, NULL);
		if (iter == NULL) {
			struct containerRecordForCapping* record = malloc(
				sizeof(struct containerRecordForCapping));
			record->cid = c->id;
			record->chunk_number = 1;
			//去除标记块后的序号，且从0开始计数
			record->firstChunkNum_isContainer = rewrite_buffer.num;
			/* We first assume it is out-of-order */
			record->out_of_order = 1;
			record->physical_address_start = c->physical_address_start;

			//用来显示区域各容器第一次出现的顺序
			//printf("appear container:%ld    ", c->id);


			g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
				record, g_recordForCapping_cmp_by_startAddress, NULL);
		}
		else {
			struct containerRecordForCapping* record = g_sequence_get(iter);
			assert(record->cid == c->id);
			record->chunk_number++;
		}
	}

	rewrite_buffer.num++;
	rewrite_buffer.size += c->size;
	return 0;
}

struct chunk* rewrite_buffer_popForCapping() {
	struct chunk* c = g_queue_pop_head(rewrite_buffer.chunk_queue);

	if (c && !CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
		&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		/* A normal chunk */
		if (CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id != TEMPORARY_ID) {
			
			/* History-Aware Rewriting */
			if (destor.rewrite_enable_har && CHECK_CHUNK(c, CHUNK_DUPLICATE))
				har_check(c);
		}
		rewrite_buffer.num--;
		rewrite_buffer.size -= c->size;
	}
	return c;
}

/*
 * return 1 if buffer is full;
 * return 0 if buffer is not full.
 */
int rewrite_buffer_push(struct chunk* c) {
	g_queue_push_tail(rewrite_buffer.chunk_queue, c);

	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END))
		return 0;

	if (c->id != TEMPORARY_ID) {
		assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
		struct containerRecord tmp_record;
		tmp_record.cid = c->id;
		GSequenceIter *iter = g_sequence_lookup(
				rewrite_buffer.container_record_seq, &tmp_record,
				g_record_cmp_by_id,
				NULL);
		if (iter == NULL) {
			struct containerRecord* record = malloc(
					sizeof(struct containerRecord));
			record->cid = c->id;
			record->size = c->size;
			/* We first assume it is out-of-order */
			record->out_of_order = 1;

			//用来显示区域各容器第一次出现的顺序
			printf("appear container:%d    ", c->id);



			g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
					record, g_record_cmp_by_id, NULL);
		} else {
			struct containerRecord* record = g_sequence_get(iter);
			assert(record->cid == c->id);
			record->size += c->size;
		}
	}

	rewrite_buffer.num++;
	rewrite_buffer.size += c->size;

	if (rewrite_buffer.num >= destor.rewrite_algorithm[1]) {
		assert(rewrite_buffer.num == destor.rewrite_algorithm[1]);
		return 1;
	}

	return 0;
}

struct chunk* rewrite_buffer_top() {
	return g_queue_peek_head(rewrite_buffer.chunk_queue);
}

struct chunk* rewrite_buffer_pop() {
	struct chunk* c = g_queue_pop_head(rewrite_buffer.chunk_queue);

	if (c && !CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
			&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		/* A normal chunk */
		if (CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id != TEMPORARY_ID) {
			GSequenceIter *iter = g_sequence_lookup(
					rewrite_buffer.container_record_seq, &c->id,
					g_record_cmp_by_id, NULL);
			assert(iter);
			struct containerRecord* record = g_sequence_get(iter);
			record->size -= c->size;
			if (record->size == 0)
				g_sequence_remove(iter);

        	/* History-Aware Rewriting */
            if (destor.rewrite_enable_har && CHECK_CHUNK(c, CHUNK_DUPLICATE))
                har_check(c);
		}
		rewrite_buffer.num--;
		rewrite_buffer.size -= c->size;
	}

	return c;
}

/*
 * If rewrite is disable.
 */
static void* no_rewrite(void* arg) {
	while (1) {
		struct chunk* c = sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		sync_queue_push(rewrite_queue, c);

        /* History-Aware Rewriting */
        if (destor.rewrite_enable_har && CHECK_CHUNK(c, CHUNK_DUPLICATE))
            har_check(c);
    }

    sync_queue_term(rewrite_queue);

    return NULL;
}

void start_rewrite_phase() {
    rewrite_queue = sync_queue_new(1000);

    init_rewrite_buffer();

    init_har();

    if (destor.rewrite_algorithm[0] == REWRITE_NO) {
        pthread_create(&rewrite_t, NULL, no_rewrite, NULL);
    } else if (destor.rewrite_algorithm[0]
            == REWRITE_CFL_SELECTIVE_DEDUPLICATION) {
        pthread_create(&rewrite_t, NULL, cfl_rewrite, NULL);
    } else if (destor.rewrite_algorithm[0] == REWRITE_CONTEXT_BASED) {
        pthread_create(&rewrite_t, NULL, cbr_rewrite, NULL);
    } else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING) {
        pthread_create(&rewrite_t, NULL, cap_rewrite_fix, NULL);
	} else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING_LOCALITY) {
		pthread_create(&rewrite_t, NULL, spatial_locality_cap_rewrite, NULL);
	} 
	/*
	else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING_MY) {
		pthread_create(&rewrite_t, NULL, cap_rewrite_my, NULL);
	}
	else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING_LBW) {
		pthread_create(&rewrite_t, NULL, cap_rewrite_lbw, NULL);
	}
	*/
	else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING_DOUBLESIZE) {
		pthread_create(&rewrite_t, NULL, cap_rewrite_doubleSize, NULL);
	}
	else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING_FCRC) {
		pthread_create(&rewrite_t, NULL, cap_rewrite_fcrc, NULL);
	}
	else {
        fprintf(stderr, "Invalid rewrite algorithm\n");
        exit(1);
    }

}

void stop_rewrite_phase() {
    pthread_join(rewrite_t, NULL);
    NOTICE("rewrite phase stops successfully!");
}
