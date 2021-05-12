
/*
 * NOTE TO STUDENTS: Before you do anything else, please
 * provide your group information here.
 *
 * Group No.: 23 (Join a project group in Canvas)
 * First member's full name: Kwok Man Hei
 * First member's email address: mhkwok22-c@my.cityu.edu.hk
 * Second member's full name: (leave blank if none)
 * Second member's email address: (leave blank if none)
 * Third member's full name: (leave blank if none)
 * Third member's email address: (leave blank if none)
 */

///////////////////Libraries///////////////////////
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include<string.h>
#include <sys/mman.h> 
#include <pthread.h>
#include <sys/stat.h> 
#include <sys/sysinfo.h>
#include <unistd.h>
#include <dirent.h>
///////////////////////////////////////////////////


/////////////////GLOBAL VARIABLES////////////////////////
int total_threads; //Total number of threads that will be created for consumer.
int page_size; //Page size = 4096 Bytes
int num_files; //Total number of the files passed as the arguments.
int isComplete=0; //Flag needed to wakeup any sleeping threads at the end of program.
int total_pages; //required for the compressed output
int q_head=0; //Circular queue head.
int q_tail=0; //Circular queue tail.
#define q_capacity 10 //Circular queue current size. We can not have static array 
//for buf which size is given as a variable, so use define. (Stackoverflow)
int q_size=0; //Circular queue capacity.
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER, filelock=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER, fill = PTHREAD_COND_INITIALIZER;
int* pages_per_file;
/////////////////////////////////////////////////////////

/////////////////STRUCTURES///////////////////////////////

//Contains the compressed output
struct output {
	char* data;
	int* count;
	int size;
}*out;

//Contains page specific data of a specific file.
struct buffer {
    char* address; //Mapping address of the file_number file + page_number page
    int file_number; //File Number
    int page_number; //Page number
    int last_page_size; //Page sized or (size_of_file)%page_size
}buf[q_capacity];

//Contains file specific data for munmap
struct fd{
	char* addr;
	int size;
}*files;
////////////////////////////////////////////////////////

/////////////////QUEUE Functions////////////////////////

//buf is the Buffer queue. Queue capacity by default = 10
//Add at q_head index of the circular queue. 
void put(struct buffer b){
  	buf[q_head] = b; //Enqueue the buffer
  	q_head = (q_head + 1) % q_capacity;
  	q_size++;
}

//Remove from q_tail index of the circular queue.
struct buffer get(){
  	struct buffer b = buf[q_tail]; //Dequeue the buffer.
	q_tail = (q_tail + 1) % q_capacity;
  	q_size--;
  	return b;
}

////////////////////////////////////////////////////////
void readF(char* fName,int fIndex){
        char* map;
		int file;
		struct stat sb;
        file = open(fName, O_RDONLY);
		int pages_in_file=0; 
		int last_page_size=0; 
		
		if(file == -1){
			exit(1);
		}

		//Step 3: Get the file info.
		if(fstat(file,&sb) == -1){ 
			close(file);
			exit(1);
		}
		
		
		pages_in_file=(sb.st_size/page_size);
		if(((double)sb.st_size/page_size)>pages_in_file){ 
			pages_in_file+=1;
			last_page_size=sb.st_size%page_size;
		}
		else{ //Page alligned
			last_page_size=page_size;
		}
		total_pages+=pages_in_file;
		pages_per_file[fIndex]=pages_in_file;
		
		map = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, file, 0); 													  
		if (map == MAP_FAILED) { 
			close(file);	
			exit(0);
    	}	
    	

		for(int j=0;j<pages_in_file;j++){
			pthread_mutex_lock(&lock);
			while(q_size==q_capacity){
			    pthread_cond_broadcast(&fill); 
				pthread_cond_wait(&empty,&lock);
			}
			pthread_mutex_unlock(&lock);
			struct buffer temp;
			if(j==pages_in_file-1){ 
				temp.last_page_size=last_page_size;
			}
			else{
				temp.last_page_size=page_size;
			}
			temp.address=map;
			temp.page_number=j;
			temp.file_number=fIndex;
			map+=page_size; //Go to next page in the memory.
			//possible race condition while changing size.
			pthread_mutex_lock(&lock);
			put(temp);
			pthread_mutex_unlock(&lock);
			pthread_cond_signal(&fill);
			close(file);
			}
}
size_t read_dir(const char * dir_path,char *** file_list){
        struct dirent *dir; 
		DIR *d = opendir(dir_path);
        size_t count=0;
		if (d==NULL){return 0;}
		*file_list=NULL;
		 char dir_path_bar[100];
         strcpy(dir_path_bar,dir_path);
         strcat(dir_path_bar,"/");
         // get the number of file under di
		 //  int count = scandir("./", &dir, NULL, alphasort );
		while ((dir = readdir(d)) != NULL) 
         { 
		 if (!strcmp (dir->d_name, "."))
            continue;
         if (!strcmp (dir->d_name, ".."))    
            continue;
         	
		 count++;
		 }

		 rewinddir(d);
		 //get file name list
		  
		  *file_list= calloc(count,sizeof(char*));
		 count=0;
		
		 while ((dir = readdir(d)) != NULL) 
         { 
		 
		 if (!strcmp (dir->d_name, "."))
            continue;
         if (!strcmp (dir->d_name, ".."))    
            continue;	

		 char* entry_path=calloc(1000,sizeof(char));
         strcpy(entry_path,dir_path_bar);
         strcat(entry_path,dir->d_name);
		 
		   (*file_list)[count]=strdup(entry_path);
          
		  count++;
		 } 
		 closedir(d);
      return count;
}

void* producer(void *arg){
	char** fNames = (char **)arg;
        size_t dir_file_NumCount =0;
        for(int i=0; i<num_files; i++)
        {
		char **files;
		size_t dir_File_Num=0;
		if((dir_File_Num=read_dir(fNames[i],&files))!=0)
		{  for(int j=0;j<dir_File_Num;j++)
		     readF(files[j],i+dir_file_NumCount+j);
			//printf("%s\n",files[j]); 
		dir_file_NumCount+=dir_File_Num;
		}else{
		readF(fNames[i],i+dir_file_NumCount);
		}
		}
        isComplete=1;
		pthread_cond_broadcast(&fill);
		return 0;
}
/////////////////////////////////////////////////////////////////////////

///////////////////////////CONSUMER/////////////////////////////////////

//Compresses the buffer object.
struct output RLECompress(struct buffer temp){
	struct output compressed;
	compressed.count=malloc(temp.last_page_size*sizeof(int));
	char* tempString=malloc(temp.last_page_size);
	int countIndex=0;

	for(int i=0;i<temp.last_page_size;i++){
	    
		tempString[countIndex]=temp.address[i];
		compressed.count[countIndex]=1;
		while(i+1<temp.last_page_size && tempString[countIndex]==temp.address[i+1]){
			compressed.count[countIndex]++;
			i++;
		}
		countIndex++;
	}
	compressed.size=countIndex;
	compressed.data=realloc(tempString,countIndex);
	return compressed;
}


//Calculates the relative output position for the buffer object.
int calculateOutputPosition(struct buffer temp){
	int position=0;

	for(int i=0;i<temp.file_number;i++){
		position+=pages_per_file[i];
	}

	position+=temp.page_number;
	return position;
}


void *consumer(){
	do{
		pthread_mutex_lock(&lock);
		while(q_size==0 && isComplete==0){
		    pthread_cond_signal(&empty);
			pthread_cond_wait(&fill,&lock); //call the producer to start filling the queue.
		}
		if(isComplete==1 && q_size==0){ //If producer is done mapping and there's nothing left in the queue.
			pthread_mutex_unlock(&lock);
			return NULL;
		}
		struct buffer temp=get();
		if(isComplete==0){
		    pthread_cond_signal(&empty);
		}	
		pthread_mutex_unlock(&lock);
		
		int position=calculateOutputPosition(temp);
		out[position]=RLECompress(temp);
	}while(!(isComplete==1 && q_size==0));
	return NULL;
}

//sum the count if last word at page equal first word at next page
void sum_last_firstCharacter_count(int i){

		if(i<total_pages-1){
			if(out[i].data[out[i].size-1]==out[i+1].data[0]){ 
				out[i+1].count[0]+=out[i].count[out[i].size-1];
				out[i].size--;
			}
		}
}
void printOutput(){
	
	for(int i=0;i<total_pages;i++){
		sum_last_firstCharacter_count(i);
		for(int j=0;j<out[i].size;j++){
			fwrite(&out[i].count[j],sizeof(int),1,stdout);
			fwrite(&out[i].data[j],sizeof(char),1,stdout);
		}
	}

}



int main(int argc, char* argv[]){
	//Check if less than two arguments
	if(argc<2){
		printf("pzip: file1 [file2 ...]\n");
		exit(1);
	}

	//Initialize all the global Variables.
	//I took 4096 as page size but the program was running very slow,
	//started trying out with huge random values, program execution
	//decreased by atleast 1/4
	page_size = 10000000;//sysconf(_SC_PAGE_SIZE); //4096 bytes
	num_files=argc-1; //Number of files, needed for producer.
	total_threads=get_nprocs(); //Number of processes consumer threads 
	pages_per_file=malloc(sizeof(int)*num_files); //Pages per file.
	

    out=malloc(sizeof(struct output)* 512000*2); 
	//Create producer thread to map all the files.
	pthread_t pid,cid[total_threads];
	pthread_create(&pid, NULL, producer, argv+1); //argv + 1 to skip argv[0].

	//Create consumer thread to compress all the pages per file.
	for (int i = 0; i < total_threads; i++) {
        pthread_create(&cid[i], NULL, consumer, NULL);
    }

    //Wait for producer-consumers to finish.
    for (int i = 0; i < total_threads; i++) {
        pthread_join(cid[i], NULL);
    }
    pthread_join(pid,NULL);
	printOutput();

	return 0;
}