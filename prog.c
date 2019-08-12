#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include <ck_ht.h>


#include <assert.h>
#include <ck_epoch.h>
#include <ck_malloc.h>
#include <ck_pr.h>
#include <ck_spinlock.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "common.h"

#define MIN(x, y) (((x) < (y)) ? (x) : (y))

static ck_ht_t ht CK_CC_CACHELINE;
static char **keys;
static size_t keys_length = 0;
static size_t keys_capacity = 128;
static ck_epoch_t epoch_ht;
static ck_epoch_record_t epoch_wr;
static int n_threads;
static bool next_stage;

enum state {
	HT_STATE_STOP = 0,
	HT_STATE_GET,
	HT_STATE_STRICT_REPLACEMENT,
	HT_STATE_DELETION,
	HT_STATE_REPLACEMENT,
	HT_STATE_COUNT
};

static struct affinity affinerator = AFFINITY_INITIALIZER;
static uint64_t accumulator[HT_STATE_COUNT];
static ck_spinlock_t accumulator_mutex = CK_SPINLOCK_INITIALIZER;
static int barrier[HT_STATE_COUNT];
static int state;

struct ht_epoch {
	ck_epoch_entry_t epoch_entry;
};

COMMON_ALARM_DECLARE_GLOBAL(ht_alarm, alarm_event, next_stage)

static void
alarm_handler(int s)
{

	(void)s;
	next_stage = true;
	return;
}

static void
ht_destroy(ck_epoch_entry_t *e)
{

	free(e);
	return;
}

static void *
ht_malloc(size_t r)
{
	ck_epoch_entry_t *b;

	b = malloc(sizeof(*b) + r);
	return b + 1;
}

static void
ht_free(void *p, size_t b, bool r)
{
	struct ht_epoch *e = p;

	(void)b;

	if (r == true) {
		/* Destruction requires safe memory reclamation. */
		ck_epoch_call(&epoch_wr, &(--e)->epoch_entry, ht_destroy);
	} else {
		free(--e);
	}

	return;
}

static struct ck_malloc my_allocator = {
	.malloc = ht_malloc,
	.free = ht_free
};

static void
table_init(void)
{
	unsigned int mode = CK_HT_MODE_BYTESTRING;

#ifdef HT_DELETE
	mode |= CK_HT_WORKLOAD_DELETE;
#endif

	//ck_epoch_init(&epoch_ht);
	//ck_epoch_register(&epoch_ht, &epoch_wr, NULL);
	common_srand48((long int)time(NULL));
	if (ck_ht_init(&ht, mode, NULL, &my_allocator, 50000, common_lrand48()) == false) {
		perror("ck_ht_init");
		exit(EXIT_FAILURE);
	}

	return;
}


static void *
table_get(const char *key, int li)
{
	ck_ht_entry_t entry;
	ck_ht_hash_t h;
	size_t l = strlen(key);

    //printf("Key is %s with len=%d\n",value, l);

	ck_ht_hash(&h, &ht, key, l);
	ck_ht_entry_key_set(&entry, key, l);
	if (ck_ht_get_spmc(&ht, h, &entry) == true){
		return ck_ht_entry_value(&entry);
    }

	return NULL;
}

static void *
table_get1(const char *key, int li)
{
	ck_ht_entry_t entry;
	ck_ht_hash_t h;
	size_t l = strlen(key);

    //printf("Key is %s with len=%d\n",value, l);

	ck_ht_hash(&h, &ht, key, l);
	ck_ht_entry_key_set(&entry, key, l);
	if (ck_ht_get_spmc(&ht, h, &entry) == true){
        printf("FOUND ENTRY IN HASH");
		return ck_ht_entry_value(&entry);
    }

	return NULL;
}

static bool
table_insert(const char *key, int li, const void* value)
{
	ck_ht_entry_t entry;
	ck_ht_hash_t h;
	size_t l = strlen(key);

	ck_ht_hash(&h, &ht, key, l);
	ck_ht_entry_set(&entry, h, key, l, value);
	return ck_ht_put_spmc(&ht, h, &entry);
}

static bool
table_replace(const char *key, int li, const void* value)
{
	ck_ht_entry_t entry;
	ck_ht_hash_t h;
	size_t l = strlen(key);

	ck_ht_hash(&h, &ht, key, l);
	ck_ht_entry_set(&entry, h, key, l, value);
	return ck_ht_set_spmc(&ht, h, &entry);
}

static size_t
table_count(void)
{

	return ck_ht_count(&ht);
}

static bool
table_reset(void)
{

	return ck_ht_reset_spmc(&ht);
}



FILE* fd;
off_t read_offset;
int q = 0;

struct range{
    int s;
    int e;
};

#define BUF 4096
#define BUFSIZE 4160

struct tuple{
    int first;
    int second;
};

typedef bool (*DeFunc)(char*,int);

int GLOBAL_MAX=0;
char MAX_WORD[64];

struct Params{
    struct range r;
    char* file;
    DeFunc p;
};

static pthread_mutex_t foo_mutex = PTHREAD_MUTEX_INITIALIZER;

void printstr(char* str, int len){
    printf("Printing String with Len=%d\n",len);
    for (int i=0;i<len;i++){
        printf("%c",str[i]);
    }
    printf("\n");
}

void getCommonWord(){
    ck_ht_iterator_t iterator; // = CK_HT_ITERATOR_INITIALIZER;

    ck_ht_entry_t *cursor;

    ck_ht_iterator_init(&iterator);

    int i=0;
	while (ck_ht_next(&ht, &iterator, &cursor) == true) {

        struct tuple* data = ck_ht_entry_value(cursor);

        int m = MIN(data->first,data->second);
        if (m > GLOBAL_MAX){
            char* word = ck_ht_entry_key(cursor);
            int len = ck_ht_entry_key_length(cursor);
            printstr(word,len);
            strncpy(MAX_WORD, word,len);
            MAX_WORD[len] = 0;
            GLOBAL_MAX = m;
        }
        i++;
	}
    printf("\nNumber of entries HASTABLE...%d",i);
}

void str_read(char* buffer, int* s, int* len)
{
    //find start
    int i=0;
    if (isalnum(buffer[i]))
        *s = 0;
    else{  
        while (!isalnum(buffer[i])){
            i++;
        }
        *s = i;
    }

    //find end
    int j=*s;
    int cnt=0;
    while (isalnum(buffer[j++])){
        cnt++;
    }
    *len = cnt;
}

bool processBuffer1(char* buffer, int len)
{
    //printstr(buffer,len);

    int i=0;
    while (i<len){
        //read from the buffer, one word at a time
        int s=0,e=0;
        str_read(buffer+i,&s,&e);

        char word[128];
        strncpy(word,buffer+i+s,e);
        //printstr(buffer+i+s,e);
        word[e] = 0;
        //printf("Word is %s\n",word);

        struct tuple* data = table_get(word,e);

        if (data == NULL){
            // word does not exist in the ck_ht
            //printf("WORD DOES NOT EXIST...create new\n");
            pthread_mutex_lock(&foo_mutex);
            if (data == NULL){
                data = (struct tuple*) malloc(sizeof(struct tuple));
                data->first = 1;
                data->second =0;
                table_insert(word, e, (void*)data);
            }
            else
            {
                data->first++;
                table_replace(word, e, (void*)data);
            }
            pthread_mutex_unlock(&foo_mutex);
        }
        else
        {
            //printf("WORD EXISTs...%s\n",word);
            pthread_mutex_lock(&foo_mutex);
            data->first++;
            table_replace(word, e, (void*)data);
            pthread_mutex_unlock(&foo_mutex);
        }
        i+=s;
        i+=(e);
    }
}

bool processBuffer2(char* buffer, int len)
{
    int i=0;
    while (i<len){
        //read from the buffer, one word at a time
        int s=0,e=0;
        str_read(buffer+i,&s,&e);
        char word[128];
        strncpy(word,buffer+i+s,e);
        //printstr(buffer+i+s,e);
        word[e] = 0;
        //printf("Word is %s\n",word);

        struct tuple* data = table_get1(word,e);

        if (data != NULL){
            // word does not exist in the ck_ht
            //printf("BUFFER2 WORD EXIST\n");
            pthread_mutex_lock(&foo_mutex);
            //printstr(buffer+i+s,e);
            data->second++;
            int m = MIN(data->first,data->second);
            if (m > GLOBAL_MAX){
                strncpy(MAX_WORD, buffer+i+s, e);
                MAX_WORD[e] = 0;
                GLOBAL_MAX = m;
            }
            
           printf("MATCH is %s...count is...%d\n",word,data->second);

            table_replace(word, e, (void*)data);
            pthread_mutex_unlock(&foo_mutex);
        }
        i+=s;
        i+=(e);
    }
}

void* reader_routine(void* p)
{
    struct Params* r = (struct Params*)p;

    char buffer[BUFSIZE] = {0};
    int bytesRead = 0;
    int totalRead = 0;
    off_t read_offset = r->r.s;
    int len = r->r.e - r->r.s;

    FILE* fp = fopen(r->file, "r");
    if (fp == NULL) { 
        printf("File Not Found!\n"); 
        return; 
    } 

    //printf("Start...%d...End%d...len...%d",r->r.s,r->r.e,len);
    //getchar();

    bool bProcessed = true;
    bool bFirst=true;

    while(totalRead+BUF < len) //is the read ahead ok??
    {
        //flock(fp, LOCK_EX);
        fseek(fp, read_offset+totalRead, SEEK_SET);
        bytesRead = fread(buffer, 1, BUF, fp);
        //printf("reader_rutine..bytes read...%d",bytesRead);
        //flock(fp, LOCK_UN);
        if (bytesRead > 0)
        {
            totalRead += bytesRead;
            //fseek(fd, read_offset + totalRead, SEEK_SET);
            
            //puts(buffer);
            if (isalnum(buffer[bytesRead-1])){ //if last character is alphanum...possible part of word
                //last character is alpha...read till space and read the word
                char buffer1[64] = {0};
                //flock(fd, LOCK_EX);
                int len = fread(buffer1, 1, 64, fd);
                int s=0,e=0;
                str_read(buffer1,&s,&e);
                //flock(fp, LOCK_UN);
                len = e;
                if (len>0){
                    //append to buffer
                    memcpy(buffer+bytesRead,buffer1,len);
                    bytesRead += len;
                    totalRead += len;
                }
            }
            buffer[bytesRead] = 0;
            //puts(buffer);
            //getchar();
            int skipper=0;
            if ( (bFirst) && (r->r.s!=0)){
                while(isalnum(buffer[skipper])){
                    skipper++;
                }
                bFirst = false;
            }
            bProcessed = r->p(buffer+skipper,bytesRead-skipper);
            memset(buffer,0,BUFSIZE);
        }
    }

    int TO_READ = len-(totalRead+BUF);
    if (TO_READ > 0){ // read the last part
        fseek(fp, read_offset+totalRead, SEEK_SET);
        
        bytesRead = fread(buffer, 1, TO_READ, fp);
        //printf("reader_rutine..bytes read...%d",bytesRead);
        //flock(fp, LOCK_UN);
        if (bytesRead > 0)
        {
            totalRead += bytesRead;
            //fseek(fd, read_offset + totalRead, SEEK_SET);
            
            //puts(buffer);
            if (isalpha(buffer[bytesRead-1])){ //if last character is alphanum...possible part of word
                //last character is alpha...read till space and read the word
                char buffer1[64] = {0};
                //flock(fd, LOCK_EX);
                int len = fread(buffer1, 1, 64, fd);
                int s=0,e=0;
                str_read(buffer1,&s,&e);
                //flock(fp, LOCK_UN);
                len = e-s;
                if (len>0){
                    //append to buffer
                    memcpy(buffer+bytesRead,buffer1,len);
                    bytesRead += len;
                    totalRead += len;
                }
            }
            buffer[bytesRead] = 0;
            //puts(buffer);
            //getchar();
            int skipper=0;
            if ( (bFirst) && (r->r.s!=0)){
                while(isalnum(buffer[skipper])){
                    skipper++;
                }
                bFirst = false;
            }
            bProcessed = r->p(buffer+skipper,bytesRead-skipper);
            memset(buffer,0,BUFSIZE);
        }
    }

    fclose(fp);

    return NULL;
}



void read_file(char* file, DeFunc F)
{
    fd = fopen(file, "r");
    if (fd == NULL) { 
        printf("File Not Found!\n"); 
        return; 
    } 
  
    fseek (fd,0L,SEEK_END);
    // calculating the size of the file 
    long int res = ftell(fd);

    fseek(fd, 0, SEEK_SET);
    fclose(fd);

    long int quanta = res/4;
    struct Params r[4];
    r[0].r.s = 0; r[0].r.e = quanta; r[0].p=F; r[0].file=file;
    r[1].r.s = quanta+1; r[1].r.e = (2*quanta);r[1].p=F;r[1].file=file;
    r[2].r.s = r[1].r.e+1; r[2].r.e = (3*quanta);r[2].p=F;r[2].file=file;
    r[3].r.s = r[2].r.e+1; r[3].r.e = res;r[3].p=F;r[3].file=file;



    //now create 4 threads per 4 core and read the files
    //pthread_t th[4];
    pthread_t th[4];

    for (int i=0;i<4;i++){
        pthread_create(&th[i], NULL, reader_routine, &r[i]);
    }

    for (int i=0;i<4;i++){
        pthread_join(th[i], NULL);
    }


    fclose(fd);
}

int main(int argc, char* argv[])
{
    table_init();

    pthread_mutex_init(&foo_mutex, NULL);

    read_file(argv[1], &processBuffer1);
    read_file(argv[2], &processBuffer2);

    //getCommonWord();

    if (GLOBAL_MAX >0){
        printf("\nCommon Word is...%s...count=%d",MAX_WORD,GLOBAL_MAX);
    }

    printf("\nEnd of Program");
}