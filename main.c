#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <mpi.h>

#define MAX_QUEUE_SIZE 3
#define NUM_PROCESSES 3

#define true  1
#define false 0
typedef uint8_t bool;

typedef int32_t  i32;
typedef uint32_t u32;
typedef uint64_t u64;

// lib {{{

typedef union {
  struct {
    u32 p1;
    u32 p2;
    u32 p3;
  };
  u32 ps[NUM_PROCESSES];
} Clock;

typedef struct {
  Clock clock;
  i32 origin;
} Task;

typedef struct {
  Task tasks[MAX_QUEUE_SIZE];
  u32 head, tail, len;
} TaskQueue;

bool task_enqueue(TaskQueue *q, Task t){
  if (q->len >= MAX_QUEUE_SIZE) {
    return false;
  }

  q->tasks[q->tail] = t;
  q->tail = (q->tail + 1) % MAX_QUEUE_SIZE;
  q->len++;

  return true;
}

bool task_dequeue(TaskQueue *q, Task *t){
  if (q->len <= 0){
    return false;
  }

  *t = q->tasks[q->head];
  q->head = (q->head + 1) % MAX_QUEUE_SIZE;
  q->len--;

  return true;
}


void task_dequeue_multithreaded(TaskQueue *q, Task *t, pthread_mutex_t *lock, pthread_cond_t *cond_has_items, pthread_cond_t *cond_has_space){
  pthread_mutex_lock(lock);
  {
    while (!task_dequeue(q, t)) {
        pthread_cond_wait(cond_has_items, lock);
    }
    pthread_cond_signal(cond_has_space);
  }
  pthread_mutex_unlock(lock);
}

void task_enqueue_multithreaded(TaskQueue *q, Task t, pthread_mutex_t *lock, pthread_cond_t *cond_has_items, pthread_cond_t *cond_has_space){
  pthread_mutex_lock(lock);
  {
    while (!task_enqueue(q, t)) {
        pthread_cond_wait(cond_has_space, lock);
    }
    pthread_cond_signal(cond_has_items);
  }
  pthread_mutex_unlock(lock);
}

void clock_print(Clock *c){
  printf("Clock (%d, %d, %d)", c->p1, c->p2, c->p3);
}

// }}} lib

pthread_mutex_t queue_in_lock, queue_out_lock;
pthread_cond_t queue_in_has_space, queue_in_has_items, queue_out_has_space, queue_out_has_items;

static TaskQueue queue_in, queue_out;
static i32 process_rank;
static Clock process_clock;

void event_internal_process(void){
  process_clock.ps[process_rank]++;
  printf("[THREAD %d] Internal process: ", process_rank);
  clock_print(&process_clock);
  printf("\n");
}

void event_consume(){

  Task out_t;
  task_dequeue_multithreaded(
    &queue_out,
    &out_t,
    &queue_out_lock,
    &queue_out_has_items,
    &queue_out_has_space
  );


  for (i32 i = 0; i < NUM_PROCESSES; ++i) {
    if (out_t.clock.ps[i] > process_clock.ps[i]) {
      process_clock.ps[i] = out_t.clock.ps[i];
    }
  }

  process_clock.ps[process_rank]++;

  printf("[THREAD %d: CONSUMER] Processed clock from %d: ", process_rank, out_t.origin);
  clock_print(&process_clock);
  printf("\n");
}

void event_produce(){

  Task in_t = {
    .clock = process_clock,
    .origin = process_rank,
  };

  task_enqueue_multithreaded(
    &queue_in,
    in_t,
    &queue_in_lock,
    &queue_in_has_items,
    &queue_in_has_space
  );

  process_clock.ps[process_rank]++;

  printf("[THREAD %d: PRODUCER] Produced clock: ", process_rank);
  clock_print(&process_clock);
  printf("\n");
}

void event_send(i32 id){

  Task t;
  task_dequeue_multithreaded(
    &queue_in,
    &t,
    &queue_in_lock,
    &queue_in_has_items,
    &queue_in_has_space
  );

  process_clock.ps[process_rank]++;

  MPI_Send(&t, sizeof(Task), MPI_BYTE, id, 0, MPI_COMM_WORLD);

  printf("[THREAD %d] Sent clock to %d: ", process_rank, id);
  clock_print(&t.clock);
  printf("\n");
}

void event_receive(i32 id){
  Task receive;
  MPI_Recv(&receive, sizeof(Task), MPI_BYTE, id, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  task_enqueue_multithreaded(
    &queue_out,
    receive,
    &queue_out_lock,
    &queue_out_has_items,
    &queue_out_has_space
  );

  for (i32 i = 0; i < NUM_PROCESSES; ++i) {
    if (receive.clock.ps[i] > process_clock.ps[i]) {
      process_clock.ps[i] = receive.clock.ps[i];
    }
  }

  process_clock.ps[process_rank]++;

  printf("[THREAD %d] Received clock from %d: ", process_rank, id);
  clock_print(&receive.clock);
  printf("\n");
}

void process0(void){ 
  event_internal_process();

  printf("[THREAD %d] ", process_rank);
  clock_print(&process_clock);
  printf("\n");

  event_send(2);
  event_send(1);
  event_send(1);
  event_receive(1);
  event_internal_process();
  event_internal_process();
  event_send(2);
  event_receive(2);
  event_send(1);
  event_internal_process();

  printf("[THREAD %d] Switching clock with thread 1: ", process_rank);
  clock_print(&process_clock);
  printf("\n");
}
void process1(void){
  printf("[THREAD %d] ", process_rank);
  clock_print(&process_clock);
  printf("\n");

  event_send(0);
  event_send(2);
  event_send(0);
  event_receive(2);
  event_internal_process();
  event_internal_process();
  event_send(2);
  event_receive(2);
  event_send(2);
  event_internal_process();
}
void process2(void){
  event_internal_process();
  printf("[THREAD %d] ", process_rank);
  clock_print(&process_clock);
  printf("\n");

  event_receive(0);
  event_send(1);
  event_receive(0);
  event_receive(1);
  event_receive(1);
  event_internal_process();
  event_internal_process();
  event_send(1);
  event_receive(1);
  event_send(0);
  event_internal_process();
}


void *consumer_thread(void *data){
  for (;;) {
    event_consume();
    sleep(1);
  }
  pthread_exit(NULL);
}

void *producer_thread(void *data){
  for (;;) {
    event_produce();
    sleep(1);
  }
  pthread_exit(NULL);
}

int main(void){
  MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
  assert(process_rank < NUM_PROCESSES);

  pthread_mutex_init(&queue_in_lock, NULL);
  pthread_cond_init(&queue_in_has_space, NULL);
  pthread_cond_init(&queue_in_has_items, NULL);

  pthread_mutex_init(&queue_out_lock, NULL);
  pthread_cond_init(&queue_out_has_space, NULL);
  pthread_cond_init(&queue_out_has_items, NULL);


  pthread_t producer_thread_id;
  pthread_create(&producer_thread_id, NULL, producer_thread, NULL);

  pthread_t consumer_thread_id;
  pthread_create(&consumer_thread_id, NULL, consumer_thread, NULL);

  switch (process_rank){
  case 0: process0();
  case 1: process1();
  case 2: process2();
  }

  MPI_Finalize();
  return 0;
}
