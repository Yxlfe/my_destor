/*
 * rewrite_phase.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#ifndef REWRITE_PHASE_H_
#define REWRITE_PHASE_H_

#include "destor.h"

struct connect_info {
	int32_t index_number; //��index_number����
	int32_t pre;
	int32_t next;
};

struct localityContainerRecord {
	GQueue* containerRecordForCapping_queue;//���containerRecordForCapping�Ķ���
	int32_t chunk_number;
};

struct containerRecordForCapping {
	containerid cid;
	int32_t chunk_number; //Ϊ��cid�����п�����
	int32_t out_of_order;
	int64_t physical_address_start;	//����������ַ��ʼ��
	//int64_t physical_address_end;	//����������ַ������
	int32_t firstChunkNum_isContainer;	//Ϊ��container�ĵ�һ�����ݿ�����(��0��ʼ)
};

struct containerRecord {
	containerid cid;
	int32_t size; //Ϊ��cid�����п��С��
	int32_t out_of_order;
};

extern struct rewrite_buffer{
	GQueue *chunk_queue;
	GSequence *container_record_seq; //��¼chunk_queue�����п����ڵ�������Ϣ���У������ǻ���ƽ�������ʵ�ֵģ�
	int num; //chunk_queue�д�ŵ�ʵ�������ݵĿ������
	int size;//chunk_queue�д�ŵ�ʵ�������ݵ����п�Ĵ�С
} rewrite_buffer;



struct trigger_chunk_entity {
	containerid id;
	int64_t byte_position;
	int number;
	int64_t size;
	GQueue *chunk_queue;
	struct trigger_chunk_entity *next;
};

extern struct rewrite_buffer_my{
	GQueue *chunk_queue;
	//Ϊ�˸�׼ȷ�ļ�����ֵ�����õĳ�ǰ���ֿ����
	GQueue *look_ahead_chunk_queue;
	GHashTable *trigger_chunk_table;	
	int64_t size;//�����������п�Ĵ�С��
	int64_t look_ahead_size; //��ǰ���������п�Ĵ�С��
	int64_t mainArea_tailPositon;
	int64_t mainArea_headPositon;
	int64_t look_ahead_tailPositon;
	int64_t look_ahead_headPositon;
	int set_chunk_outOfOrder_count;
} rewrite_buffer_my;

void* cfl_rewrite(void* arg);
void* cbr_rewrite(void* arg);
void* cap_rewrite(void* arg);
//=======�Լ����ӵ�
void* cap_rewrite_fix(void* arg);
void* spatial_locality_cap_rewrite(void* arg);
void* cap_rewrite_doubleSize(void* arg);
void* cap_rewrite_fcrc(void* arg);
void* cap_rewrite_my(void* arg);
void* cap_rewrite_lbw(void* arg);

/* har_rewrite.c */
void init_har();
void close_har();
void har_monitor_update(containerid id, int32_t size);
void har_check(struct chunk* c);

/* restore_aware.c */
void init_restore_aware();
void restore_aware_update(containerid id, int32_t chunklen);
int restore_aware_contains(containerid id);
double restore_aware_get_cfl();

/* For sorting container records. */
gint g_record_descmp_by_length(struct containerRecord* a,
		struct containerRecord* b, gpointer user_data);
glong g_record_cmp_by_id(struct containerRecord* a, struct containerRecord* b,
		gpointer user_data);

//Ϊ������containerRecordForCapping
glong g_recordForCapping_cmp_by_startAddress(struct containerRecordForCapping* a,
		struct containerRecordForCapping* b, gpointer user_data);
gint g_recordForCapping_cmp_by_chunkNumber(struct containerRecordForCapping* a,
		struct containerRecordForCapping* b, gpointer user_data);

//Ϊ������localityContainerRecord
gint g_localityContainerRecord_cmp_by_number(struct localityContainerRecord *a,
		struct localityContainerRecord *b, gpointer user_data);


void init_rewrite_buffer_my();
struct trigger_chunk_entity*  new_trigger_chunk_entity(int64_t id, int64_t byte_position);
void free_trigger_chunk_entity(gpointer data);
void set_chunk_outoforder(gpointer data, gpointer userdata) ;
int rewrite_buffer_pushTo_lookAhead(struct chunk *c);
int rewrite_buffer_pushForMy(struct chunk *c, GQueue *queque);
struct chunk* rewrite_buffer_popForMy(int *n_cap, int *n_rewrite);

int rewrite_buffer_pushForLBW(struct chunk *c, int tc);
struct chunk* rewrite_buffer_popForLBW();



int rewrite_buffer_push(struct chunk* c);

int rewrite_buffer_pushForCapping(struct chunk *c);


struct chunk* rewrite_buffer_popForCapping();

struct chunk* rewrite_buffer_pop();
struct chunk* rewrite_buffer_top();

#endif /* REWRITE_PHASE_H_ */
