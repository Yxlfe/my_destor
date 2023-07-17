/*
 * recipestore.h
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#ifndef RECIPESTORE_H_
#define RECIPESTORE_H_

#include "../destor.h"

/*
 * A backup version
 * A backup version describes the fingerprint sequence of a backup job to facilitate restore jobs.
 * It consists of three metadata files: .meta, .recipe, and .record.
 * The .record file is for the optimal cache: it consists of access records of referred containers.
 * The .recipe file records the fingerprint sequence of the backup with segment boundary indicators.
 * Hence, the .recipe file consists of segment recipes (each of which describes the
 * fingerprint sequence of a segment).
 * The .meta file consists of metadata of file recipes, i.e., fileRecipeMeta.
 * Each fileRecipeMeta structure describes the range of a file recipe in the .recipe file.
 * So, we can accurately restore a file.
 * */
struct backupVersion {

	sds path;
	int32_t bv_num; /* backup version number start from 0 */

	int deleted;

	int64_t number_of_files;
	int64_t number_of_chunks;

	sds fname_prefix; /* The prefix of the file names */

	FILE *metadata_fp;
	FILE *recipe_fp;
	FILE *record_fp;

	/* the write buffer of recipe meta */
	char *metabuf;
	int metabufoff;

	/* the write buffer of records */
	char *recordbuf;
	int recordbufoff;
	
	//要被写入recipe文件中的段缓冲区
	char* segmentbuf;
	int segmentlen;
	int segmentbufoff;
};

/* Point to the meta of a file recipe */
struct fileRecipeMeta {
	int64_t chunknum;
	int64_t filesize;
	sds filename;
};

/*
 * Each recipe consists of segments.
 * Each prefetched segment is organized as a hash table for optimizing lookup.
 * It is the basic unit of logical locality.
 * 该数据结构用作预取段过程中
 * */
struct segmentRecipe {
	segmentid id;
	/* Map fingerprints in the segment to their container IDs.*/
	GHashTable *kvpairs;
};

/*
 * If id == CHUNK_SEGMENT_START or CHUNK_SEGMENT_END,
 * it is a flag of segment boundary.
 * If id == CHUNK_SEGMENT_START,
 * size indicates the length of the segment in terms of # of chunks.
 * 该数据结构为真实的写入recipe文件需要的数据结构
 */
struct chunkPointer {
	fingerprint fp;
	containerid id;
	int32_t size;
};

//自己考虑局部性时  真实的写入recipe文件需要的数据结构
struct chunkPointerForLocality {
	fingerprint fp;
	containerid id;
	int32_t size;
	int16_t pre;
	int16_t next;
};

void init_recipe_store();
void close_recipe_store();

struct backupVersion* create_backup_version(const char *path);
int backup_version_exists(int number);
struct backupVersion* open_backup_version(int number);
void update_backup_version(struct backupVersion *b);
void free_backup_version(struct backupVersion *b);

void append_file_recipe_meta(struct backupVersion* b, struct fileRecipeMeta* r);
void append_n_chunk_pointers(struct backupVersion* b,
		struct chunkPointer* cp, int n);
//自己加的
void append_n_chunk_pointers_forLocality(struct backupVersion* b,
	struct chunkPointerForLocality* cp, int n);

struct fileRecipeMeta* read_next_file_recipe_meta(struct backupVersion* b);
struct chunkPointer* read_next_n_chunk_pointers(struct backupVersion* b, int n,
		int *k);
//自己加的
struct chunkPointerForLocality* read_next_n_chunk_pointers_forLocality(struct backupVersion* b,
		int n, int *k);

containerid* read_next_n_records(struct backupVersion* b, int n, int *k);
struct fileRecipeMeta* new_file_recipe_meta(char* name);
void free_file_recipe_meta(struct fileRecipeMeta* r);

int segment_recipe_check_id(struct segmentRecipe* sr, segmentid *id);
struct segmentRecipe* new_segment_recipe();
void free_segment_recipe(struct segmentRecipe* sr);
segmentid append_segment_flag(struct backupVersion* b, int flag, int segment_size);
//自己加的
segmentid append_segment_flag_forLocality(struct backupVersion* b, int flag, int segment_size);

GQueue* prefetch_segments(segmentid id, int prefetch_num);
int lookup_fingerprint_in_segment_recipe(struct segmentRecipe* sr,
        fingerprint *fp);

struct segmentRecipe* read_next_segment(struct backupVersion *bv);

#endif /* RECIPESTORE_H_ */
