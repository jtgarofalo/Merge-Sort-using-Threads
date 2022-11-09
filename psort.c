#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sys/sysinfo.h>

struct record {
  int key;
  char data[96];
};

struct bounds{
  int lowerBound;
  int upperBound;
  pthread_t tid;
  struct record *array;
};

void merge(struct record *rec, int left, int middle, int right)
{
   // variable used to create temp arrays
  int var1 = middle - left + 1;
  int var2 = right - middle; 

  //temp arrays
  struct record *rec1, *rec2;
  
  rec1 = malloc(sizeof(struct record) * var1);
  rec2 = malloc(sizeof(struct record) * var2);

  for(int i = 0; i < var1; i++){
    rec1[i].key = rec[left+i].key;
    memcpy(rec1[i].data, rec[left+i].data, 96);
  }
  for(int j = 0; j < var2; j++){
    rec2[j].key = rec[middle + 1 +j].key;
    memcpy(rec2[j].data, rec[middle + 1 +j].data, 96);
  }

  int i = 0;
  int j = 0;
  int temp = left;

  while(i < var1 && j < var2) {
    if(rec1[i].key <= rec2[j].key){
      rec[temp].key = rec1[i].key;
      memcpy(rec[temp].data, rec1[i].data, 96);
      i++;
    }
    else {
      rec[temp].key = rec2[j].key;
      memcpy(rec[temp].data, rec2[j].data, 96);
      j++;
    }
    temp++;
  }

  while(i < var1) {//Loops through the rest of the remaining elements
    rec[temp].key = rec1[i].key;
    memcpy(rec[temp].data, rec1[i].data, 96);
    i++;
    temp++;
  }

  while(j < var2) { //Loops through the rest of the remaining elements
    rec[temp].key = rec2[j].key;
    memcpy(rec[temp].data, rec2[j].data, 96);
    j++;
    temp++;
  }
  free(rec1);
  free(rec2);
}

void mergeSort(struct record rec[], int left, int right) 
{
  if(left < right) {
    int middle;
    middle = left + (right-left)/2;

    mergeSort(rec, left, middle);
    mergeSort(rec, middle+1, right);

    merge(rec, left, middle, right);
  }
}

//This method takesa struct, which holds an upper and lower bound, which are indexes of our record array
//the threads that call this method will be responsible for sorting their section of
//our record array
void * threadSort(void *arguments){
  struct bounds *args = arguments;
  mergeSort(args->array, args->lowerBound, args->upperBound);
  return 0;
}

int main(int argc, char *argv[]) {
  //clock_t start,threadStart, threadEnd,readStart, readEnd, end;        //initialize clock to time program
  //double cpu_time_used, read_time_used, thread_time_used;      //same as above
  //start = clock();
  //readStart = clock();
  if(argc != 3) {
    fprintf( stderr, "An error has occurred\n");
    exit(0);
  }
  FILE *fp;
  int fd;
  fp = fopen(argv[1], "r");
  if(fp == NULL){
    fprintf( stderr, "An error has occurred\n");
    exit(0);
  }
  fd = fileno(fp);
  if(fd < 0) {
    fprintf( stderr, "An error has occurred\n");
    exit(0);
  }
  
  struct stat data;
  int err = fstat(fd, &data);
  if(err < 0) {
    fprintf( stderr, "An error has occurred\n");
    exit(0);
  }

  char *ptr = mmap(NULL, data.st_size, PROT_READ, MAP_SHARED, fd, 0);

  if(ptr == MAP_FAILED) {
    fprintf( stderr, "An error has occurred\n");
    exit(0);
  }
  fclose(fp);
  int numRecords = data.st_size/100;
  struct record *rec;
  rec = (struct record *) malloc(data.st_size);
  memcpy(rec, ptr, data.st_size);
  //readEnd = clock();
  //threadStart = clock();
  //added thread creation logic below
  int n = get_nprocs();                           //amount of threads we will be using
  if(n % 2 == 1){                                 //ensures even amount of processes, important so each thread has its own block of memory
    n = n - 1;
  }
  struct bounds threadArg[n];          //initialize struct array n length long, so we have a different struct ptr for each thread
  int segSize = numRecords / n;        //amount of records each thread will be responsible for sorting
  for(int i = 0; i < n; i++){
    if(i + 1 ==n){                     //This if statement checks if this is the last thread created, and sets the upper bound to account for any extra records
      threadArg[i].lowerBound = (i*segSize);
      threadArg[i].upperBound = ((i + 1)*segSize)- 1 + (numRecords % n);
      threadArg[i].array = rec;
    }
    else{
      threadArg[i].lowerBound = (i*segSize);                        //sets lower bound of sorting
      threadArg[i].upperBound = ((i + 1)*segSize)-1;                //sets upper bound of sorting
      threadArg[i].array = rec;                                    //i THINK this passes the base pointer of the rec struct array
    }
    pthread_create(&threadArg[i].tid, NULL, threadSort, (void *)&threadArg[i]);//creates thread 
  }

  //wait for threads to finish before continuing
  for(int i = 0; i < n; i++){
    pthread_join(threadArg[i].tid, NULL);
  }

  //SECOND THREAD CREATION LOOP
  if(n >= 2){
    n = n / 2;                           //HALVES AMOUNT OF THREADS
    struct bounds threadArg2[n];          //initialize struct array n length long, so we have a different struct ptr for each thread
    segSize = numRecords / n;        //amount of records each thread will be responsible for sorting
    for(int i = 0; i < n; i++){
      if(i + 1 ==n){                     //This if statement checks if this is the last thread created, and sets the upper bound to account for any extra records
        threadArg2[i].lowerBound = (i*segSize);
        threadArg2[i].upperBound = ((i + 1)*segSize)- 1 + (numRecords % n);
        threadArg2[i].array = rec;
      }
      else{
        threadArg2[i].lowerBound = (i*segSize);                        //sets lower bound of sorting
        threadArg2[i].upperBound = ((i + 1)*segSize)-1;                //sets upper bound of sorting
        threadArg2[i].array = rec;                                    //i THINK this passes the base pointer of the rec struct array
      }
      pthread_create(&threadArg2[i].tid, NULL, threadSort, (void *)&threadArg[i]);//creates thread 
    }

    //wait for threads to finish before continuing
    for(int i = 0; i < n; i++){
      pthread_join(threadArg2[i].tid, NULL);
    }
  }
  //END OF SECOND THREAD CREATION LOOP
  
  //thread creation logic above

  //FINAL ITERATION - just calls threadSort on our array
  struct bounds threadArg3;
  threadArg3.lowerBound = 0;
  threadArg3.upperBound = numRecords - 1;
  threadArg3.array = rec;
  threadSort(&threadArg3);                                                //calls threadSort one final time using one thread essentially
  //threadEnd = clock();

  fp = fopen(argv[2], "w");
  fd = fileno(fp);

  if(fd < 0) {
    fprintf( stderr, "An error has occurred\n");
    exit(0);
  }

  for(int i = 0; i < numRecords; i++) {
    fwrite(&rec[i], sizeof(struct record), 1, fp);
    //int x = rec[i].key;
    //printf("RecordNum: %d, key Val: %d\n", i, x);
  }
  
  fclose(fp);
  //end = clock();
  //cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
  //thread_time_used = ((double) (threadEnd - threadStart)) / CLOCKS_PER_SEC;
  //read_time_used = ((double) (readEnd - readStart)) / CLOCKS_PER_SEC;
  //printf("Total time run: %f\n", cpu_time_used);
  //printf("Thread creation / execution time: %f\n", thread_time_used);
  //printf("Time to read and map input: %f\n", read_time_used);
  return 0;
}