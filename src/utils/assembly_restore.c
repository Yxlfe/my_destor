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
	//���ݴ������С������
	int32_t num = b->size - a->size;
	return num == 0 ? 1 : num;	//���Դ��size��ͬ��Ԫ��
}

static void init_assembly_area() {
	//ע������û��ָ���ͷ��ڴ�ռ�Ĺ�������(NULL)
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
	UPA��ͷ������䷽����
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

	//�����Լ���
	g_queue_push_tail(assembly_area.upa, c);

	gboolean flag = g_hash_table_contains(assembly_area.upa_trigger_table, &c->id);
	if (flag == FALSE) {
		//��������������û���䷽�е�id��Ŀʱ���򽫸ÿ���Ϊ�����飬Ϊ���½���Ŀ���봥����

		struct trigger_chunk* trigger =
			new_trigger_chunk(c->id, assembly_area.upa_head_position);
		g_queue_push_tail(trigger->chunk_queue, c);
		trigger->size = c->size;
		g_hash_table_insert(assembly_area.upa_trigger_table, &trigger->id, trigger);
	}
	else {
		//�������������д��ڸ�id����Ŀʱ���ÿ�Ϊ�Ǵ����飬���ÿ���ӵ���Ӧ����Ŀ������

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
	UPA��β���Ƴ��䷽����
	flagָ���ǵ����Ŀ��Ƿ�Ϊ�����飬��flagΪ1ʱ��������Ϊ�����飬Ϊ0�����Ǵ�����
	triggerΪ��UPA��������ɾ���Ĵ�����ṹ��ָ���ָ��
	�˺���ÿ����һ���䷽�󣬶��Զ���UPA���ٴ�����
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
		/*�����Ŀ���ڣ��Ҹÿ��ֽ�λ�õ��ڲ�ѯ������Ŀ���ֽ�λ�ã�
		���ÿ�Ϊһ��������ʱ���򽫸���Ŀ�Ӹ�����������ɾ��
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

	/*ÿ���Ƴ�һ���䷽�󶼻ὫUPA���ٴ�����*/
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
	SPA��ͷ������䷽������Ҳ�ǹ�����ͷ������䷽������
	�˺�������Ĭ�ϴ�UPA��β�������䷽����������ӵ�SPA��
	�˺���ÿ����SPA�����һ���䷽
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
	//UPA���Ѿ�����
	if (c == NULL) {
		return -1;
	}

	if (assembly_area.transition_area_size + c->size > assembly_area.transition_area_maxSize)
		return 1;

	//�����Լ���
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
			//�����Ŀ���ڣ��򽫴�UPA������ɾ������Ŀ��������������������Ӧ��Ŀ����β��
			//temp_triggerָ����Ŀ����β��
			while (temp_trigger->next != NULL) {
				temp_trigger = temp_trigger->next;
			}
			temp_trigger->next = trigger;
		}
		else {
			//�����Ŀ�����ڣ��򽫴�UPA������ɾ������Ŀ������������������
			g_hash_table_insert(assembly_area.spa_trigger_table, &trigger->id, trigger);
		}
	}
	else {
		//����ÿ�Ϊ�Ǵ����飬�����������������
	}

	return 0;
}

/*
	������β���Ƴ��䷽����
	�˺���ÿ����һ���䷽�󣬶��Զ����������ٴ�����
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

	/*ÿ�ε���һ���䷽�󣬶����Զ���������*/
	while (1) {
		int result = transition_area_push();
		if (result != 0) {
			break;
		}
	}

	return c;
}

/*
	ASM��ͷ������䷽����
	�˺�������Ĭ�ϴӹ����������䷽����������ӵ�ASM��
	�˺���ÿ����ASM�����һ���䷽
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
	//�����������Ѿ�����
	if (c == NULL) {
		return -1;
	}

	if (assembly_area.size + c->size > assembly_area.area_size)
		return 1;
	//�����Լ���

	c = transition_area_pop();
	g_sequence_append(assembly_area.area, c);
	assembly_area.size += c->size;
	assembly_area.num++;
	return 0;
}

/*
	SPA��β���Ƴ��䷽������Ҳ��ASM��β���Ƴ��䷽������
*/
static struct chunk* asm_pop(GSequenceIter *begin) {
	struct chunk *c = g_sequence_get(begin);
	struct trigger_chunk* trigger =
		g_hash_table_lookup(assembly_area.spa_trigger_table, &c->id);
	if (trigger != NULL && assembly_area.spa_tail_position == trigger->byte_position) {
		/*�����Ŀ���ڣ��Ҹÿ��ֽ�λ�õ��ڲ�ѯ������Ŀ�������ϴ����飨��ͷ�����ֽ�λ��ʱ,
		���ÿ�Ϊһ��������*/

		if (CHECK_CHUNK(c, CHUNK_CACHED)) {
			//�����������������п鶼���Ϊready������Ŀ�Ľ�ɫת��Ϊ��ǰ���Ŀ��ɫ��
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
					//�ӻ������ɾ���ô������¼
					g_sequence_remove(iter);
					break;
				}
			}
			assert(temp_trigger != NULL);
			assert(temp_trigger == trigger);
			assembly_area.cache_size -= temp_trigger->size;
			assembly_area.used_size += temp_trigger->size;

			//�ٽ��ô�����Ӹ�����������ɾ��
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
			//���ô�����Ӹ�����������ɾ��
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
		//�������ʱ�����ÿ�Ϊһ���Ǵ����飬��ֱ���Ƴ��䷽
	}

	g_sequence_remove(begin);
	assembly_area.size -= c->size;
	assembly_area.num--;
	assembly_area.spa_tail_position += c->size;
	assembly_area.used_size -= c->size;
	return c;
}

/*
	�����������
	�˺���ÿ���������������ռ���ڴ�������Ŀ
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
	���溯��
	�Ի���Ĵ�����Ĵ�СΪ���ȼ�
*/
static void cache(struct container *con) {
	int64_t id = con->meta.id;
	struct trigger_chunk* trigger =
		g_hash_table_lookup(assembly_area.spa_trigger_table, &id);
	while (trigger != NULL) {
		if (trigger->is_cached == 0) {
			if (assembly_area.used_size + assembly_area.cache_size +
				trigger->size <= assembly_area.area_size) {
				/*������������������ݴ�С + ��ռ�õ��ڴ��С <= �涨���ڴ�ռ��С
				ֱ�ӻ��津������������Ӧ�Ŀ�����*/
				g_queue_foreach(trigger->chunk_queue, set_chunk_data, con);
				trigger->is_cached = 1;
				g_sequence_insert_sorted(assembly_area.cache_table,
					trigger, g_trigger_chunk_cmp_by_size, NULL);
				assembly_area.cache_size += trigger->size;
			}
			else {
				/*������������������ݴ�С + ��ռ�õ��ڴ��С > �涨���ڴ�ռ��С*/
				struct trigger_chunk* max_trigger = NULL;
				GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.cache_table);
				max_trigger = g_sequence_get(begin);
				assert(max_trigger != NULL);
				if (trigger->size < max_trigger->size) {
					/*�����������������ݴ�С < �����ѻ��津������ռ���ڴ�����һ��������ʱ��
					�����û����������Ȼ���ٻ��津������������Ӧ�Ŀ�����*/
					expel_cache();
					g_queue_foreach(trigger->chunk_queue, set_chunk_data, con);
					trigger->is_cached = 1;
					g_sequence_insert_sorted(assembly_area.cache_table,
						trigger, g_trigger_chunk_cmp_by_size, NULL);
					assembly_area.cache_size += trigger->size;
				}
				else {
					/*�����������������ݴ�С >= �����ѻ��津������ռ���ڴ�����һ��������ʱ��
					������ô�����Ļ��湤����*/
				}

			}
		}
		trigger = trigger->next;
	}
}

/*
	��ǿ��ǰװ�����ú���
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

	/*�����һ������CHUNK_CACHED��ǣ���ֱ��ִ�з��Ͳ���*/
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
	//�����ǲ��Ǽ�鲻�����һ��Ԫ�أ��ǲ���Ӧ����sequence�ĳ���������ѭ������������
	for (; iter != end; iter = g_sequence_iter_next(iter)) {
		c = g_sequence_get(iter);
		//����������IDΪid�����ݿ������������chunk.data��
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
			&& id == c->id) {
			if (destor.simulation_level == SIMULATION_NO) {

				while (assembly_area.used_size + assembly_area.cache_size +
					c->size > assembly_area.area_size) {
					//ִ�л����������
					expel_cache();
				}
				struct chunk *buf = get_chunk_in_container(con, &c->fp);
				assert(c->size == buf->size);
				c->data = malloc(c->size);
				memcpy(c->data, buf->data, c->size);
				free_chunk(buf);
				//Ϊ����used_size
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

	/*���津�������������ݿ�*/
	cache(con);

	return q;
}




/*
 * ���Ǿֲ���ʱ
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
	//�ֱ��¼��ǰ���������һ����ǰ��ͺ�������λ�����ڵ���������
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
	//��¼��һ�����ַ�����Ķε���ʼ����id
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
		//�����ǲ��Ǽ�鲻�����һ��Ԫ�أ��ǲ���Ӧ����sequence�ĳ���������ѭ������������
		for (; iter != end; iter = g_sequence_iter_next(iter)) {
			c = g_sequence_get(iter);
			//printf("=== ready going to check_chunk!!!!!!!!!!\n");
			//����������IDΪcon[j].meta.id�����ݿ������������chunk.data��
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
	//�����ǲ��Ǽ�鲻�����һ��Ԫ�أ��ǲ���Ӧ����sequence�ĳ���������ѭ������������
	for (;iter != end;iter = g_sequence_iter_next(iter)) {
		c = g_sequence_get(iter);
		//����������IDΪid�����ݿ������������chunk.data��
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& id == c->id) {
			if (destor.simulation_level == SIMULATION_NO) {
				struct chunk *buf = get_chunk_in_container(con, &c->fp);
				assert(c->size == buf->size);
				c->data = malloc(c->size);
				memcpy(c->data, buf->data, c->size);
				free_chunk(buf);

				//Ϊ����cache_size
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
			//Ϊ�˼���cache_size
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
			//�����ǲ��Ǽ�鲻�����һ��Ԫ�أ��ǲ���Ӧ����sequence�ĳ���������ѭ������������
			for (; iter != end; iter = g_sequence_iter_next(iter)) {
				c = g_sequence_get(iter);
				//����������IDΪid�����ݿ������������chunk.data��
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
���Ǿֲ���ʱ����ԭdestor�����������ڣ�are���򲻻ᳬ�����ô�С
����ǰװ������������䷽���ݵĿ飬
��ǰװ����δ��ʱ����0������ʱ����1
���ļ���ʼ�ͽ�����־�Ŀ鲻�����С����ֻ�з���Ŀ��ܴ�С
	���ڻ���� ���õĴ�Сʱ�Ż���Ϊ���ˣ�
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

	//�����Լ���
	g_sequence_append(assembly_area.area, c);
	//����ţ���0��ʼ����ΪKey����chunk��Ϊvalue�����ϣ��
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
����ǰװ������������䷽���ݵĿ飬
��ǰװ����δ��ʱ����0������ʱ����1
���ļ���ʼ�ͽ�����־�Ŀ鲻�����С����ֻ�з���Ŀ��ܴ�С
	���ڻ���� ���õĴ�Сʱ�Ż���Ϊ���ˣ�
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

	//�����Լ���
	g_sequence_append(assembly_area.area, c);
	assembly_area.size += c->size;
	assembly_area.num++;

	return 0;
}
/*
��ǰװ���㷨
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
	//�������һ��Ϊ����assembly_area.area���䷽��Ϣ
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
��ǿ��ǰװ���㷨
*/
void* enhance_assembly_restore_thread(void *arg) {
	init_assembly_area();
	struct chunk* c;
	/*������UPA*/
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

	/*����������*/
	while (1) {
		int result = transition_area_push();
		if (result != 0) {
			break;
		}
	}

	
	while (1) {
		/*����ASM��*/
		while (1) {
			int result = asm_push();
			if (result != 0) {
				break;
			}
		}

		GQueue *q = enhance_assemble_area();

		/*�����䷽����������*/
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
��ǰװ���㷨,��������Ϊ����
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
	//�������һ��Ϊ����assembly_area.area���䷽��Ϣ
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
���Ǿֲ���ʱ����ǰװ���㷨
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
	
	//�������һ��δ����assembly_area.area���䷽��Ϣ
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
