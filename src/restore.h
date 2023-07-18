/*
 * restore.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#ifndef RESTORE_H_
#define RESTORE_H_

#include "utils/sync_queue.h"

extern struct assembly_area{
	GSequence *area; //ASM区域
	int num;	//记录ASM区域中实际块的数量
	int64_t area_size;	//ASM区域最大大小设置
	int64_t size; //ASM当前区域大小
	
	GHashTable* hashtable;	//此处是为asm-locality

	//自己加的
	int64_t spa_tail_position;	//稳定区SPA尾部字节位置，也是ASM区尾部字节位置

	GQueue *transition_area; //过渡区域
	int64_t transition_area_maxSize; //过渡区最大大小
	int64_t transition_area_size;	//过渡区域当前大小
	int64_t spa_head_position;	//稳定区SPA头部字节位置，也是过渡区头部字节位置

	GQueue *upa;	//非稳定区UPA
	int64_t upa_maxSize; //非稳定区最大大小
	int64_t upa_size;	//非稳定区当前大小
	int64_t upa_head_position;	//非稳定区头部字节位置
	int64_t upa_tail_position;	//非稳定去尾部字节位置

	GHashTable* spa_trigger_table;	//稳定区触发表
	GHashTable* upa_trigger_table;	//非稳定区触发表
	GSequence *cache_table;	//缓存表

	int64_t used_size;//由ASM装配的所有块占用内存的大小
	int64_t cache_size;//由缓存触发块的所有块占用的内存的大小
} assembly_area;

struct trigger_chunk {
	containerid id;
	int64_t byte_position;
	int size;
	int is_cached;
	GQueue *chunk_queue;
	struct trigger_chunk *next;
};




extern SyncQueue *restore_chunk_queue;
extern SyncQueue *restore_recipe_queue;

//自己加的
void* assembly_restore_thread(void *arg);
void* enhance_assembly_restore_thread(void *arg);
void* assembly_restore_thread_unitContainer(void *arg);
void* locality_assembly_restore_thread(void *arg);


void* optimal_restore_thread(void *arg);

#endif /* RESTORE_H_ */
