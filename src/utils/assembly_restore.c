/*
 * assembly_restore.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "restore.h"

static struct trigger_chunk*  new_trigger_chunk(int64_t id, int64_t byte_position) {
	struct trigger_chunk* trigger = malloc(sizeof(struct trigger_chunk));
	trigger->id = id;
	//printf("===new a trigger_entity ,it's byte position is %ld\n",byte_position);
	trigger->byte_position = byte_position;
	trigger->size = 0;
	trigger->is_cached = 0;
	trigger->chunk_queue = g_queue_new();
	trigger->next = NULL;
	return trigger;
}
static void free_trigger_chunk(gpointer data) {
	struct trigger_chunk* p = data;
	g_queue_free(p->chunk_queue);
	p->chunk_queue = NULL;
	free(p);
}

static void set_chunk_flag_fromChunkCached(gpointer data, gpointer userdata) {
	struct chunk *c = (struct chunk*)data;
	UNSET_CHUNK(c, CHUNK_CACHED);
	int *flag = (int *)userdata;
	SET_CHUNK(c, *flag);
}

static void clear_data_flag(gpointer data, gpointer userdata) {
	struct chunk *c = (struct chunk*)data;
	free(c->data);
	c->data = NULL;
	int *flag = (int *)userdata;
	UNSET_CHUNK(c, *flag);
}

static void set_chunk_data(gpointer data, gpointer userdata) {
	struct chunk *c = (struct chunk*)data;
	struct container *con = (struct container*)userdata;
	struct chunk *buf = get_chunk_in_container(con, &c->fp);
	assert(c->size == buf->size);
	c->data = malloc(c->size);
	memcpy(c->data, buf->data, c->size);
	free_chunk(buf);
	SET_CHUNK(c, CHUNK_CACHED);
}

static gint g_trigger_chunk_cmp_by_size(struct trigger_chunk* a,
	struct trigger_chunk* b, gpointer user_data) {
	//根据触发块大小倒序排
	int32_t num = b->size - a->size;
	return num == 0 ? 1 : num;	//可以存放size相同的元素
}

static void init_assembly_area() {
	//注意这里没有指定释放内存空间的关联函数(NULL)
	assembly_area.area = g_sequence_new(NULL);
	assembly_area.num = 0;
	assembly_area.area_size = (destor.restore_cache[1] - 1) * CONTAINER_SIZE;
	assembly_area.size = 0;

	assembly_area.spa_tail_position = 0;
	assembly_area.transition_area = g_queue_new();
	assembly_area.transition_area_maxSize = (destor.restore_cache[2] -
		2 * (destor.restore_cache[1] - 1)) * CONTAINER_SIZE;
	assembly_area.transition_area_size = 0;
	assembly_area.spa_head_position = 0;
	assembly_area.upa = g_queue_new();
	assembly_area.upa_maxSize = (destor.restore_cache[1] - 1) * CONTAINER_SIZE;
	assembly_area.upa_size = 0;
	assembly_area.upa_head_position = 0;
	assembly_area.upa_tail_position = 0;
	assembly_area.spa_trigger_table = 
		g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free_trigger_chunk);
	assembly_area.upa_trigger_table = 
		g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, NULL);
	assembly_area.cache_table = g_sequence_new(NULL);
	assembly_area.used_size = 0;
	assembly_area.cache_size = 0;
}
/*
	UPA区头部添加配方函数
*/
static int upa_push(struct chunk* c) {
	/* Indicates end */
	if (c == NULL)
		return -1;

	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
		g_queue_push_tail(assembly_area.upa, c);
		return 0;
	}

	if (assembly_area.upa_size + c->size > assembly_area.upa_maxSize)
		return 1;

	//还可以加入
	g_queue_push_tail(assembly_area.upa, c);

	gboolean flag = g_hash_table_contains(assembly_area.upa_trigger_table, &c->id);
	if (flag == FALSE) {
		//当此区触发表中没有配方中的id条目时，则将该块视为触发块，为其新建条目插入触发表；

		struct trigger_chunk* trigger =
			new_trigger_chunk(c->id, assembly_area.upa_head_position);
		g_queue_push_tail(trigger->chunk_queue, c);
		trigger->size = c->size;
		g_hash_table_insert(assembly_area.upa_trigger_table, &trigger->id, trigger);
	}
	else {
		//当此区触发表中存在该id的条目时，该块为非触发块，将该块添加到相应的条目数据中

		struct trigger_chunk* trigger =
			g_hash_table_lookup(assembly_area.upa_trigger_table, &c->id);
		g_queue_push_tail(trigger->chunk_queue, c);
		trigger->size += c->size;
	}

	assembly_area.upa_size += c->size;
	assembly_area.upa_head_position += c->size;

	return 0;
}

/*
	UPA区尾部移除配方函数
	flag指针标记弹出的块是否为触发块，当flag为1时代表弹出的为触发块，为0则代表非触发块
	trigger为从UPA触发表中删除的触发块结构体指针的指针
	此函数每弹出一个配方后，都自动将UPA区再次填满
*/
static struct chunk* upa_pop(int* flag, struct trigger_chunk** trigger) {
	struct chunk* c = NULL;
	while (c = g_queue_pop_head(assembly_area.upa)) {
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_queue_push_tail(assembly_area.transition_area, c);
			continue;
		}
		break;
	}
	if (c == NULL) {
		return NULL;
	}

	struct trigger_chunk* temp_trigger =
		g_hash_table_lookup(assembly_area.upa_trigger_table, &c->id);
	if (temp_trigger != NULL && assembly_area.upa_tail_position == temp_trigger->byte_position) {
		/*如果条目存在，且该块字节位置等于查询出来条目的字节位置，
		即该块为一个触发块时，则将该条目从该区触发表中删除
		*/

		*flag = 1;
		*trigger = temp_trigger;
		g_hash_table_remove(assembly_area.upa_trigger_table, &c->id);
	}
	else {
		*flag = 0;
	}

	assembly_area.upa_size -= c->size;
	assembly_area.upa_tail_position += c->size;

	/*每次移除一个配方后都会将UPA区再次填满*/
	struct chunk* c_temp;
	while ((c_temp = sync_queue_get_top(restore_recipe_queue))) {
		if (upa_push(c_temp)) {
			/* Full */
			break;
		}
		else {
			sync_queue_pop(restore_recipe_queue);
		}
	}

	return c;
}

/*
	SPA区头部添加配方函数（也是过渡区头部添加配方函数）
	此函数内置默认从UPA区尾部弹出配方，并将其添加到SPA区
	此函数每次向SPA区添加一个配方
*/
static int transition_area_push() {
	struct chunk* c = NULL;
	while (c = g_queue_peek_head(assembly_area.upa)) {
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_queue_push_tail(assembly_area.transition_area, c);
			g_queue_pop_head(assembly_area.upa);
			continue;
		}
		break;
	}
	//UPA区已经空了
	if (c == NULL) {
		return -1;
	}

	if (assembly_area.transition_area_size + c->size > assembly_area.transition_area_maxSize)
		return 1;

	//还可以加入
	int flag = -1;
	struct trigger_chunk* trigger = NULL;
	c = upa_pop(&flag, &trigger);
	g_queue_push_tail(assembly_area.transition_area, c);
	assembly_area.spa_head_position += c->size;
	assembly_area.transition_area_size += c->size;

	if (flag == 1) {
		struct trigger_chunk* temp_trigger =
			g_hash_table_lookup(assembly_area.spa_trigger_table, &c->id);
		if (temp_trigger != NULL) {
			//如果条目存在，则将从UPA触发表删除的条目数据添加至该区触发表对应条目链的尾部
			//temp_trigger指向条目链的尾部
			while (temp_trigger->next != NULL) {
				temp_trigger = temp_trigger->next;
			}
			temp_trigger->next = trigger;
		}
		else {
			//如果条目不存在，则将从UPA触发表删除的条目数据新添到该区触发表
			g_hash_table_insert(assembly_area.spa_trigger_table, &trigger->id, trigger);
		}
	}
	else {
		//如果该块为非触发块，则不用做其他特殊操作
	}

	return 0;
}

/*
	过渡区尾部移除配方函数
	此函数每弹出一个配方后，都自动将过渡区再次填满
*/
static struct chunk* transition_area_pop() {
	struct chunk* c = NULL;
	while (c = g_queue_pop_head(assembly_area.transition_area)) {
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_sequence_append(assembly_area.area, c);
			continue;
		}
		break;
	}
	if (c == NULL) {
		return NULL;
	}

	assembly_area.transition_area_size -= c->size;

	/*每次弹出一个配方后，都会自动填满该区*/
	while (1) {
		int result = transition_area_push();
		if (result != 0) {
			break;
		}
	}

	return c;
}

/*
	ASM区头部添加配方函数
	此函数内置默认从过渡区弹出配方，并将其添加到ASM区
	此函数每次向ASM区添加一个配方
*/
static int asm_push() {
	struct chunk* c = NULL;
	while (c = g_queue_peek_head(assembly_area.transition_area)) {
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_sequence_append(assembly_area.area, c);
			g_queue_pop_head(assembly_area.transition_area);
			continue;
		}
		break;
	}
	//过渡区域区已经空了
	if (c == NULL) {
		return -1;
	}

	if (assembly_area.size + c->size > assembly_area.area_size)
		return 1;
	//还可以加入

	c = transition_area_pop();
	g_sequence_append(assembly_area.area, c);
	assembly_area.size += c->size;
	assembly_area.num++;
	return 0;
}

/*
	SPA区尾部移除配方函数（也是ASM区尾部移除配方函数）
*/
static struct chunk* asm_pop(GSequenceIter *begin) {
	struct chunk *c = g_sequence_get(begin);
	struct trigger_chunk* trigger =
		g_hash_table_lookup(assembly_area.spa_trigger_table, &c->id);
	if (trigger != NULL && assembly_area.spa_tail_position == trigger->byte_position) {
		/*如果条目存在，且该块字节位置等于查询出来条目链中最老触发块（链头）的字节位置时,
		即该块为一个触发块*/

		if (CHECK_CHUNK(c, CHUNK_CACHED)) {
			//将触发块引导的所有块都标记为ready（缓存的块的角色转变为向前填充的块角色）
			int flag = CHUNK_READY;
			g_queue_foreach(trigger->chunk_queue,
				set_chunk_flag_fromChunkCached, &flag);
			trigger->is_cached = 0;

			struct trigger_chunk* temp_trigger = NULL;
			GSequenceIter *iter = g_sequence_get_begin_iter(assembly_area.cache_table);
			GSequenceIter *end = g_sequence_get_end_iter(assembly_area.cache_table);
			for (; iter != end; iter = g_sequence_iter_next(iter)) {
				temp_trigger = g_sequence_get(iter);
				assert(temp_trigger != NULL);
				if (temp_trigger->byte_position == trigger->byte_position) {
					//从缓存表中删除该触发块记录
					g_sequence_remove(iter);
					break;
				}
			}
			assert(temp_trigger != NULL);
			assert(temp_trigger == trigger);
			assembly_area.cache_size -= temp_trigger->size;
			assembly_area.used_size += temp_trigger->size;

			//再将该触发块从该区触发表中删除
			struct trigger_chunk* new_trigger = trigger->next;
			if (new_trigger == NULL) {
				g_hash_table_remove(assembly_area.spa_trigger_table, &c->id);
			}
			else {
				g_hash_table_replace(assembly_area.spa_trigger_table,
					&new_trigger->id, new_trigger);
			}
		}

		if (CHECK_CHUNK(c, CHUNK_READY)) {
			//将该触发块从该区触发表中删除
			struct trigger_chunk* new_trigger = trigger->next;
			if (new_trigger == NULL) {
				g_hash_table_remove(assembly_area.spa_trigger_table, &c->id);
			}
			else {
				g_hash_table_replace(assembly_area.spa_trigger_table,
					&new_trigger->id, new_trigger);
			}
		}
	}
	else {
		//其他情况时，即该块为一个非触发块，则直接移除配方
	}

	g_sequence_remove(begin);
	assembly_area.size -= c->size;
	assembly_area.num--;
	assembly_area.spa_tail_position += c->size;
	assembly_area.used_size -= c->size;
	return c;
}

/*
	缓存逐出函数
	此函数每次逐出缓存序列中占用内存最大的条目
*/
static void expel_cache() {
	struct trigger_chunk* trigger = NULL;
	GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.cache_table);
	GSequenceIter *end = g_sequence_get_end_iter(assembly_area.cache_table);
	if (begin != end) {
		trigger = g_sequence_get(begin);
		int flag = CHUNK_CACHED;
		g_queue_foreach(trigger->chunk_queue, clear_data_flag, &flag);
		trigger->is_cached = 0;
		g_sequence_remove(begin);
		assembly_area.cache_size -= trigger->size;
	}
}

/*
	缓存函数
	以缓存的触发块的大小为优先级
*/
static void cache(struct container *con) {
	int64_t id = con->meta.id;
	struct trigger_chunk* trigger =
		g_hash_table_lookup(assembly_area.spa_trigger_table, &id);
	while (trigger != NULL) {
		if (trigger->is_cached == 0) {
			if (assembly_area.used_size + assembly_area.cache_size +
				trigger->size <= assembly_area.area_size) {
				/*如果触发块引导的数据大小 + 已占用的内存大小 <= 规定的内存空间大小
				直接缓存触发块引导的相应的块数据*/
				g_queue_foreach(trigger->chunk_queue, set_chunk_data, con);
				trigger->is_cached = 1;
				g_sequence_insert_sorted(assembly_area.cache_table,
					trigger, g_trigger_chunk_cmp_by_size, NULL);
				assembly_area.cache_size += trigger->size;
			}
			else {
				/*如果触发块引导的数据大小 + 已占用的内存大小 > 规定的内存空间大小*/
				struct trigger_chunk* max_trigger = NULL;
				GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.cache_table);
				max_trigger = g_sequence_get(begin);
				assert(max_trigger != NULL);
				if (trigger->size < max_trigger->size) {
					/*当触发块引导的数据大小 < 所有已缓存触发块中占用内存最大的一个触发块时，
					则启用缓存逐出程序，然后再缓存触发块引导的相应的块数据*/
					expel_cache();
					g_queue_foreach(trigger->chunk_queue, set_chunk_data, con);
					trigger->is_cached = 1;
					g_sequence_insert_sorted(assembly_area.cache_table,
						trigger, g_trigger_chunk_cmp_by_size, NULL);
					assembly_area.cache_size += trigger->size;
				}
				else {
					/*当触发块引导的数据大小 >= 所有已缓存触发块中占用内存最大的一个触发块时，
					则放弃该触发块的缓存工作。*/
				}

			}
		}
		trigger = trigger->next;
	}
}

/*
	增强向前装配适用函数
*/
static GQueue* enhance_assemble_area() {
	static int64_t byte_position = 0;

	/*Return NULL if area is empty.*/
	if (g_sequence_get_length(assembly_area.area) == 0)
		return NULL;

	GQueue *q = g_queue_new();

	struct chunk *c = NULL;
	GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.area);
	GSequenceIter *end = g_sequence_get_end_iter(assembly_area.area);
	for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
		c = g_sequence_get(begin);
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, c);
			c = NULL;
		}
		else {
			break;
		}
	}

	/* !c == true    indicates no more chunks in the area. */
	if (!c)
		return q;

	/*如果第一个块是CHUNK_CACHED标记，则直接执行发送步骤*/
	if (CHECK_CHUNK(c, CHUNK_CACHED)) {
		/* issue the assembled area */
		struct chunk* c;
		begin = g_sequence_get_begin_iter(assembly_area.area);
		c = asm_pop(begin);
		g_queue_push_tail(q, c);
		byte_position += c->size;
		
		printf("At %ld(B),max ASM-cache size is %.4f(MB), +++ use cache: container %lld \n", byte_position, assembly_area.used_size / 1024.0 / 1024.0, id);

		begin = g_sequence_get_begin_iter(assembly_area.area);
		end = g_sequence_get_end_iter(assembly_area.area);
		for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
			struct chunk *rc = g_sequence_get(begin);
			if (CHECK_CHUNK(rc, CHUNK_FILE_START) || CHECK_CHUNK(rc, CHUNK_FILE_END)) {
				g_sequence_remove(begin);
				g_queue_push_tail(q, rc);
			}
			else if (CHECK_CHUNK(rc, CHUNK_READY)) {
				rc = asm_pop(begin);
				g_queue_push_tail(q, rc);
				byte_position += rc->size;
			}
			else {
				break;
			}
		}
		return q;
	}

	/* need read a container */
	containerid id = c->id;
	struct container *con = NULL;
	jcr.read_container_num++;
	VERBOSE("At %ld(B), Restore cache: container %lld is missed", byte_position, id);
	if (destor.simulation_level == SIMULATION_NO)
		con = retrieve_container_by_id(id);

	/* assemble the area */
	GSequenceIter *iter = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	//这里是不是检查不到组后一个元素，是不是应该用sequence的长度来控制循环次数？？？
	for (; iter != end; iter = g_sequence_iter_next(iter)) {
		c = g_sequence_get(iter);
		//将所有容器ID为id的数据块的数据流填充进chunk.data域
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
			&& id == c->id) {
			if (destor.simulation_level == SIMULATION_NO) {

				while (assembly_area.used_size + assembly_area.cache_size +
					c->size > assembly_area.area_size) {
					//执行缓存逐出程序
					expel_cache();
				}
				struct chunk *buf = get_chunk_in_container(con, &c->fp);
				assert(c->size == buf->size);
				c->data = malloc(c->size);
				memcpy(c->data, buf->data, c->size);
				free_chunk(buf);
				//为计算used_size
				assembly_area.used_size += c->size;
			}
			SET_CHUNK(c, CHUNK_READY);
		}
	}

	printf("At %ld(B),max ASM-cache size is %.4f(MB), Restore cache: container %lld is missed \n", byte_position, assembly_area.used_size / 1024.0 / 1024.0, id);

	/* issue the assembled area */
	begin = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
		struct chunk *rc = g_sequence_get(begin);
		if (CHECK_CHUNK(rc, CHUNK_FILE_START) || CHECK_CHUNK(rc, CHUNK_FILE_END)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
		}
		else if (CHECK_CHUNK(rc, CHUNK_READY)) {
			rc = asm_pop(begin);
			g_queue_push_tail(q, rc);
			byte_position += rc->size;
		}
		else {
			break;
		}
	}

	/*缓存触发块引导的数据块*/
	cache(con);

	return q;
}




/*
 * 考虑局部性时
 * Forward assembly.
 * Return a queue of assembled chunks.
 * Return NULL if area is empty.
 */
static GQueue* locality_assemble_area() {

	/*Return NULL if area is empty.*/
	if (g_sequence_get_length(assembly_area.area) == 0)
		return NULL;

	GQueue *q = g_queue_new();

	struct chunk *c = NULL;
	GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.area);
	GSequenceIter *end = g_sequence_get_end_iter(assembly_area.area);
	for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
		c = g_sequence_get(begin);
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, c);
			c = NULL;
		}
		else {
			break;
		}
	}

	/* !c == true    indicates no more chunks in the area. */
	if (!c)
		return q;
	
	printf("start count container_num_sum!\n");
	
	/* read a container or a lot of containers*/
	//分别记录当前分析区域第一个块前面和后面物理位置相邻的容器个数
	int container_num_front = 0;
	int container_num_behind = 0;
	struct chunk* temp = c;
	printf("first actualy containerid is %ld\n", temp->id);
	while (temp->pre >= 0) {
		int key = (int)temp->pre;
		printf("temp->pre is %d \n", key);
		temp = g_hash_table_lookup(assembly_area.hashtable, &key);
		assert(temp);
		container_num_front++;
	}
	//记录这一物理地址连续的段的起始容器id
	containerid start_cId = temp->id;
	temp = c;
	while (temp->next >= 0) {
		int key = (int)temp->next;
		printf("temp->next is %d \n", key);
		temp = g_hash_table_lookup(assembly_area.hashtable, &key);
		assert(temp);
		container_num_behind++;
	}
	int container_num_sum = 1 + container_num_front + container_num_behind;
	int array_len = 0;
	struct container *con = NULL;
//	VERBOSE("Restore cache: container %lld is missed", id);
	if (destor.simulation_level == SIMULATION_NO)
		con = retrieve_container_multiple(start_cId, container_num_sum, &array_len);
	jcr.read_container_num += array_len;

	printf("array_len is %d==============\n", array_len);

	/* assemble the area */
	for (int j = 0; j < array_len; j++) {
		GSequenceIter *iter = g_sequence_get_begin_iter(assembly_area.area);
		end = g_sequence_get_end_iter(assembly_area.area);
		printf("con[%d].meta.id  is  %ld==============\n", j,  con[j].meta.id);
		printf("con[%d].meta.data_size  is  %d==============\n", j, con[j].meta.data_size);
		printf("con[%d].meta.chunk_num  is  %d==============\n", j, con[j].meta.chunk_num);
		//这里是不是检查不到组后一个元素，是不是应该用sequence的长度来控制循环次数？？？
		for (; iter != end; iter = g_sequence_iter_next(iter)) {
			c = g_sequence_get(iter);
			//printf("=== ready going to check_chunk!!!!!!!!!!\n");
			//将所有容器ID为con[j].meta.id的数据块的数据流填充进chunk.data域
			if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& con[j].meta.id == c->id) {
				if (destor.simulation_level == SIMULATION_NO) {
					//printf("=== ready going to get_chunk!!!!!!!!!!\n");
					struct chunk *buf = get_chunk_in_container(con+j, &c->fp);
					//printf("=== ready going to use buf!!!!!!!!!!\n");
					assert(c->size == buf->size);
					c->data = malloc(c->size);
					memcpy(c->data, buf->data, c->size);
					free_chunk(buf);
				}
				SET_CHUNK(c, CHUNK_READY);
			}
		}
	}
	printf("=== ready going to cissue the assembled area====\n");
	/* issue the assembled area */
	begin = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
		struct chunk *rc = g_sequence_get(begin);
		if (CHECK_CHUNK(rc, CHUNK_FILE_START) || CHECK_CHUNK(rc, CHUNK_FILE_END)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
		}
		else if (CHECK_CHUNK(rc, CHUNK_READY)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
			assembly_area.size -= rc->size;
			assembly_area.num--;
		}
		else {
			break;
		}
	}
	printf("queue's length is %d !\n", g_queue_get_length(q));
	printf("=== locality_assemble_area is end====\n");
	return q;
}

/*
 * Forward assembly.
 * Return a queue of assembled chunks.
 * Return NULL if area is empty.
 */
static GQueue* assemble_area() {
	static int64_t byte_position = 0;

	/*Return NULL if area is empty.*/
	if (g_sequence_get_length(assembly_area.area) == 0)
		return NULL;

	GQueue *q = g_queue_new();

	struct chunk *c = NULL;
	GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.area);
	GSequenceIter *end = g_sequence_get_end_iter(assembly_area.area);
	for (;begin != end;begin = g_sequence_get_begin_iter(assembly_area.area)) {
		c = g_sequence_get(begin);
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, c);
			c = NULL;
		} else {
			break;
		}
	}

	/* !c == true    indicates no more chunks in the area. */
	if (!c)
		return q;

	/* read a container */
	containerid id = c->id;
	struct container *con = NULL;
	jcr.read_container_num++;
	VERBOSE("At %ld(B), Restore cache: container %lld is missed",byte_position, id);
	if (destor.simulation_level == SIMULATION_NO)
		con = retrieve_container_by_id(id);

	/* assemble the area */
	GSequenceIter *iter = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	//这里是不是检查不到组后一个元素，是不是应该用sequence的长度来控制循环次数？？？
	for (;iter != end;iter = g_sequence_iter_next(iter)) {
		c = g_sequence_get(iter);
		//将所有容器ID为id的数据块的数据流填充进chunk.data域
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& id == c->id) {
			if (destor.simulation_level == SIMULATION_NO) {
				struct chunk *buf = get_chunk_in_container(con, &c->fp);
				assert(c->size == buf->size);
				c->data = malloc(c->size);
				memcpy(c->data, buf->data, c->size);
				free_chunk(buf);

				//为计算cache_size
				assembly_area.used_size += c->size;
			}
			SET_CHUNK(c, CHUNK_READY);
		}
	}

	printf("At %ld(B),max ASM-cache size is %.4f(MB), Restore cache: container %lld is missed \n", byte_position, assembly_area.used_size / 1024.0 / 1024.0, id);

	/* issue the assembled area */
	begin = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	for (;begin != end;begin = g_sequence_get_begin_iter(assembly_area.area)) {
		struct chunk *rc = g_sequence_get(begin);
		if (CHECK_CHUNK(rc, CHUNK_FILE_START) || CHECK_CHUNK(rc, CHUNK_FILE_END)){
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
		}else if(CHECK_CHUNK(rc, CHUNK_READY)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
			assembly_area.size -= rc->size;
			assembly_area.num--;
			byte_position += rc->size;
			//为了计算cache_size
			assembly_area.used_size -= rc->size;
		} else {
			break;
		}
	}
	return q;
}

static GQueue* assemble_area_unitContainer() {
	int size_assembled = 0;
	int flag = 0;
	GQueue *q = g_queue_new();
	while (size_assembled < 4 * 1024 * 1024) {
		/*Return NULL if area is empty.*/
		if (g_sequence_get_length(assembly_area.area) == 0)
			return NULL;

		struct chunk *c = NULL;
		GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.area);
		GSequenceIter *end = g_sequence_get_end_iter(assembly_area.area);
		for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
			c = g_sequence_get(begin);
			if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
				g_sequence_remove(begin);
				g_queue_push_tail(q, c);
				c = NULL;
			}
			else if (CHECK_CHUNK(c, CHUNK_READY)) {
				g_sequence_remove(begin);
				g_queue_push_tail(q, c);
				assembly_area.size -= c->size;
				assembly_area.num--;
				size_assembled += c->size;
				if (size_assembled >= 4 * 1024 * 1024) {
					flag = 1;
					break;
				}
			}
			else{
				break;
			}
		}
		if (flag != 1) {
			/* !c == true    indicates no more chunks in the area. */
			if (!c)
				return q;

			/* read a container */
			containerid id = c->id;
			struct container *con = NULL;
			jcr.read_container_num++;
			VERBOSE("Restore cache: container %lld is missed", id);
			if (destor.simulation_level == SIMULATION_NO)
				con = retrieve_container_by_id(id);

			/* assemble the area */
			GSequenceIter *iter = g_sequence_get_begin_iter(assembly_area.area);
			end = g_sequence_get_end_iter(assembly_area.area);
			//这里是不是检查不到组后一个元素，是不是应该用sequence的长度来控制循环次数？？？
			for (; iter != end; iter = g_sequence_iter_next(iter)) {
				c = g_sequence_get(iter);
				//将所有容器ID为id的数据块的数据流填充进chunk.data域
				if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& id == c->id) {
					if (destor.simulation_level == SIMULATION_NO) {
						struct chunk *buf = get_chunk_in_container(con, &c->fp);
						assert(c->size == buf->size);
						c->data = malloc(c->size);
						memcpy(c->data, buf->data, c->size);
						free_chunk(buf);
					}
					SET_CHUNK(c, CHUNK_READY);
				}
			}

			/* issue the assembled area */
			begin = g_sequence_get_begin_iter(assembly_area.area);
			end = g_sequence_get_end_iter(assembly_area.area);
			for (; begin != end; begin = g_sequence_get_begin_iter(assembly_area.area)) {
				struct chunk *rc = g_sequence_get(begin);
				if (CHECK_CHUNK(rc, CHUNK_FILE_START) || CHECK_CHUNK(rc, CHUNK_FILE_END)) {
					g_sequence_remove(begin);
					g_queue_push_tail(q, rc);
				}
				else if (CHECK_CHUNK(rc, CHUNK_READY)) {
					g_sequence_remove(begin);
					g_queue_push_tail(q, rc);
					assembly_area.size -= rc->size;
					assembly_area.num--;
					size_assembled += rc->size;
					if (size_assembled >= 4 * 1024 * 1024)
						break;
				}
				else {
					break;
				}
			}
		}
		

	}

	return q;
}

/*
考虑局部性时，与原destor函数区别在于，are区域不会超过设置大小
往向前装配区放入带有配方数据的块，
向前装配区未满时返回0；满了时返回1
（文件开始和结束标志的块不计入大小，且只有放入的块总大小
	大于或等于 设置的大小时才会认为满了）
*/
static int locality_assembly_area_push(struct chunk* c) {

	/* Indicates end */
	if (c == NULL)
		return 1;

	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
		g_sequence_append(assembly_area.area, c);
		return 0;
	}

	if (assembly_area.size + c->size > assembly_area.area_size)
		return 1;

	//还可以加入
	g_sequence_append(assembly_area.area, c);
	//将序号（从0开始）作为Key，将chunk作为value放入哈希表
/*	if(assembly_area.num == 987){
		printf("while assembly_area.num == 987,the c->id is %ld\n",c->id);
	}
*/
	if(c->pre >= 0 || c->next >= 0){
		int* key = malloc(sizeof(int));
		*key = assembly_area.num;
		g_hash_table_insert(assembly_area.hashtable, key, c);
	}
/*	if(assembly_area.num == 987){
		struct chunk* cc = g_hash_table_lookup(assembly_area.hashtable, &assembly_area.num);
		if(cc == NULL){
			printf("the chunk of assembly_area.num == 987 insert into hashtable 					erro\n");
		}
		printf("while assembly_area.num == 987,the c->id is %ld\n",c->id);
	}
*/
	assembly_area.size += c->size;
	assembly_area.num++;
	return 0;
}

/*
往向前装配区放入带有配方数据的块，
向前装配区未满时返回0；满了时返回1
（文件开始和结束标志的块不计入大小，且只有放入的块总大小
	大于或等于 设置的大小时才会认为满了）
*/
static int assembly_area_push(struct chunk* c) {
	/* Indicates end */
	if (c == NULL)
		return 1;

	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
		g_sequence_append(assembly_area.area, c);
		return 0;
	}

	if (assembly_area.size + c->size > assembly_area.area_size)
		return 1;

	//还可以加入
	g_sequence_append(assembly_area.area, c);
	assembly_area.size += c->size;
	assembly_area.num++;

	return 0;
}
/*
向前装配算法
*/
void* assembly_restore_thread(void *arg) {
	init_assembly_area();

	struct chunk* c;
	while ((c = sync_queue_get_top(restore_recipe_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (assembly_area_push(c)) {
			/* Full */
			GQueue *q = assemble_area();

			TIMER_END(1, jcr.read_chunk_time);

			struct chunk* rc;
			while ((rc = g_queue_pop_head(q))) {
				if (CHECK_CHUNK(rc, CHUNK_FILE_START) 
						|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
					sync_queue_push(restore_chunk_queue, rc);
					continue;
				}
				jcr.data_size += rc->size;
				jcr.chunk_num++;
				if (destor.simulation_level >= SIMULATION_RESTORE) {
					/* Simulating restore. */
					free_chunk(rc);
				} else {
					sync_queue_push(restore_chunk_queue, rc);
				}
			}

			g_queue_free(q);
		} 
		else {
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_pop(restore_recipe_queue);
		}

	}

	assembly_area_push(NULL);
	//处理最后一批为填满assembly_area.area的配方信息
	GQueue *q;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	while ((q = assemble_area())) {
		TIMER_END(1, jcr.read_chunk_time);
		struct chunk* rc;
		while ((rc = g_queue_pop_head(q))) {

			if (CHECK_CHUNK(rc,CHUNK_FILE_START) 
					|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
				sync_queue_push(restore_chunk_queue, rc);
				continue;
			}

			jcr.data_size += rc->size;
			jcr.chunk_num++;
			if (destor.simulation_level >= SIMULATION_RESTORE) {
				/* Simulating restore. */
				free_chunk(rc);
			} else {
				sync_queue_push(restore_chunk_queue, rc);
			}
		}
		TIMER_BEGIN(1);
		g_queue_free(q);
	}
	TIMER_END(1, jcr.read_chunk_time);

	sync_queue_term(restore_chunk_queue);
	return NULL;
}


/*
增强向前装配算法
*/
void* enhance_assembly_restore_thread(void *arg) {
	init_assembly_area();
	struct chunk* c;
	/*先填满UPA*/
	while ((c = sync_queue_get_top(restore_recipe_queue))) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (upa_push(c)) {
			break;
		}
		else {
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_pop(restore_recipe_queue);
		}
	}

	/*填满过渡区*/
	while (1) {
		int result = transition_area_push();
		if (result != 0) {
			break;
		}
	}

	
	while (1) {
		/*填满ASM区*/
		while (1) {
			int result = asm_push();
			if (result != 0) {
				break;
			}
		}

		GQueue *q = enhance_assemble_area();

		/*所有配方都处理完了*/
		if (q == NULL) {
			break;
		}

		struct chunk* rc;
		while ((rc = g_queue_pop_head(q))) {
			if (CHECK_CHUNK(rc, CHUNK_FILE_START)
				|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
				sync_queue_push(restore_chunk_queue, rc);
				continue;
			}
			jcr.data_size += rc->size;
			jcr.chunk_num++;
			if (destor.simulation_level >= SIMULATION_RESTORE) {
				/* Simulating restore. */
				free_chunk(rc);
			}
			else {
				sync_queue_push(restore_chunk_queue, rc);
			}
		}

		g_queue_free(q);

	}

	sync_queue_term(restore_chunk_queue);
	return NULL;
}


/*
向前装配算法,发送粒度为容器
*/
void* assembly_restore_thread_unitContainer(void *arg) {
	init_assembly_area();

	struct chunk* c;
	while ((c = sync_queue_get_top(restore_recipe_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (assembly_area_push(c)) {
			/* Full */
			GQueue *q = assemble_area_unitContainer();

			TIMER_END(1, jcr.read_chunk_time);

			struct chunk* rc;
			while ((rc = g_queue_pop_head(q))) {
				if (CHECK_CHUNK(rc, CHUNK_FILE_START)
					|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
					sync_queue_push(restore_chunk_queue, rc);
					continue;
				}
				jcr.data_size += rc->size;
				jcr.chunk_num++;
				if (destor.simulation_level >= SIMULATION_RESTORE) {
					/* Simulating restore. */
					free_chunk(rc);
				}
				else {
					sync_queue_push(restore_chunk_queue, rc);
				}
			}

			g_queue_free(q);
		}
		else {
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_pop(restore_recipe_queue);
		}

	}

	assembly_area_push(NULL);
	//处理最后一批为填满assembly_area.area的配方信息
	GQueue *q;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	while ((q = assemble_area_unitContainer())) {
		TIMER_END(1, jcr.read_chunk_time);
		struct chunk* rc;
		while ((rc = g_queue_pop_head(q))) {

			if (CHECK_CHUNK(rc, CHUNK_FILE_START)
				|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
				sync_queue_push(restore_chunk_queue, rc);
				continue;
			}

			jcr.data_size += rc->size;
			jcr.chunk_num++;
			if (destor.simulation_level >= SIMULATION_RESTORE) {
				/* Simulating restore. */
				free_chunk(rc);
			}
			else {
				sync_queue_push(restore_chunk_queue, rc);
			}
		}
		TIMER_BEGIN(1);
		g_queue_free(q);
	}
	TIMER_END(1, jcr.read_chunk_time);

	sync_queue_term(restore_chunk_queue);
	return NULL;
}

/*
考虑局部性时，向前装配算法
*/
void* locality_assembly_restore_thread(void *arg) {
	init_assembly_area();

	struct chunk* c;
	while ((c = sync_queue_get_top(restore_recipe_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (locality_assembly_area_push(c)) {
			TIMER_END(1, jcr.read_chunk_time);

			printf("assembly_area.num is %d \n", assembly_area.num);
			printf("assembly_area.size is %ld \n", assembly_area.size);
			printf("the length of assembly_area.hashtable is %d \n", 
				g_hash_table_size(assembly_area.hashtable));
			printf("assembly_area.hashtable information is :\n");
			GList* listKey = g_hash_table_get_keys(assembly_area.hashtable);
			GList* start = g_list_first(listKey);
			struct chunk* cc = NULL;
			while(start != NULL ){
				int* key = start->data;
				printf("key = %d   // ", *key);
				cc = g_hash_table_lookup(assembly_area.hashtable, key);
				printf("value: chunck.pre = %d   //", cc->pre);
				printf("value: chunck.next = %d   // \n", cc->next);
				start = start->next;
			}
			
			/* Full */
			while (assembly_area.size > 0) {
				printf("=====will going to locality_assemble_area !\n");
				TIMER_BEGIN(1);
				GQueue *q = locality_assemble_area();

				TIMER_END(1, jcr.read_chunk_time);

				struct chunk* rc;
				printf("==========will going to queue_pop !\n");
				while ((rc = g_queue_pop_head(q))) {
					if (CHECK_CHUNK(rc, CHUNK_FILE_START)
						|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
						sync_queue_push(restore_chunk_queue, rc);
						continue;
					}
					jcr.data_size += rc->size;
					jcr.chunk_num++;
					if (destor.simulation_level >= SIMULATION_RESTORE) {
						/* Simulating restore. */
						free_chunk(rc);
					}
					else {
						sync_queue_push(restore_chunk_queue, rc);
					}
				}

				g_queue_free(q);

			}
			printf("==========once analyze finnished!\n");
			g_hash_table_remove_all(assembly_area.hashtable);
			
		}
		else {
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_pop(restore_recipe_queue);
		}

	}

	locality_assembly_area_push(NULL);
	
	printf("assembly_area.num is %d \n", assembly_area.num);
	printf("assembly_area.size is %ld \n", assembly_area.size);
	printf("the length of assembly_area.hashtable is %d \n", 
		g_hash_table_size(assembly_area.hashtable));
	printf("assembly_area.hashtable information is :\n");
	GList* listKey = g_hash_table_get_keys(assembly_area.hashtable);
	GList* start = g_list_first(listKey);
	struct chunk* cc = NULL;
	while(start != NULL ){
		int* key = start->data;
		printf("key = %d   //", *key);
		cc = g_hash_table_lookup(assembly_area.hashtable, key);
		printf("value: chunck.pre = %d   //", cc->pre);
		printf("value: chunck.next = %d   // \n", cc->next);
		start = start->next;
	}
	
	//处理最后一批未填满assembly_area.area的配方信息
	GQueue *q;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	while ((q = locality_assemble_area())) {
		TIMER_END(1, jcr.read_chunk_time);
		struct chunk* rc;
		while ((rc = g_queue_pop_head(q))) {

			if (CHECK_CHUNK(rc, CHUNK_FILE_START)
				|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
				sync_queue_push(restore_chunk_queue, rc);
				continue;
			}

			jcr.data_size += rc->size;
			jcr.chunk_num++;
			if (destor.simulation_level >= SIMULATION_RESTORE) {
				/* Simulating restore. */
				free_chunk(rc);
			}
			else {
				sync_queue_push(restore_chunk_queue, rc);
			}
		}
		TIMER_BEGIN(1);
		g_queue_free(q);
	}
	TIMER_END(1, jcr.read_chunk_time);
	g_hash_table_remove_all(assembly_area.hashtable);

	sync_queue_term(restore_chunk_queue);
	return NULL;
}
