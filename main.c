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
  i32 destination;
} Task;

typedef struct {
  Task tasks[MAX_QUEUE_SIZE];
  u32 head, tail, len;
} TaskQueue;

typedef struct {
  TaskQueue queue;
  pthread_mutex_t lock;
  pthread_cond_t cond_has_items;
  pthread_cond_t cond_has_space;
} MultiThreadedTaskQueue;

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


void task_dequeue_multithreaded(MultiThreadedTaskQueue *q, Task *t){
  pthread_mutex_lock(&q->lock);
  {
    while (!task_dequeue(&q->queue, t)) {
        pthread_cond_wait(&q->cond_has_items, &q->lock);
    }
    pthread_cond_signal(&q->cond_has_space);
  }
  pthread_mutex_unlock(&q->lock);
}

void task_enqueue_multithreaded(MultiThreadedTaskQueue *q, Task t){
  pthread_mutex_lock(&q->lock);
  {
    while (!task_enqueue(&q->queue, t)) {
        pthread_cond_wait(&q->cond_has_space, &q->lock);
    }
    pthread_cond_signal(&q->cond_has_items);
  }
  pthread_mutex_unlock(&q->lock);
}

void clock_print(Clock *c){
  printf("Clock (%d, %d, %d)", c->ps[0], c->ps[1], c->ps[2]);
}

// }}} lib

static MultiThreadedTaskQueue queue_in, queue_out;
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
  task_dequeue_multithreaded(&queue_out, &out_t);
  MPI_Send(&out_t, sizeof(Task), MPI_BYTE, out_t.destination, 0, MPI_COMM_WORLD);

  for (i32 i = 0; i < NUM_PROCESSES; ++i) {
    if (out_t.clock.ps[i] > process_clock.ps[i]) {
      process_clock.ps[i] = out_t.clock.ps[i];
    }
  }

  // printf("[THREAD %d: CONSUMER] Processed clock from %d: ", process_rank, out_t.origin);
  // clock_print(&process_clock);
  // printf("\n");
}

void event_produce(){

  Task receive;
  MPI_Recv(&receive, sizeof(Task), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  task_enqueue_multithreaded(&queue_in, receive);

  // printf("[THREAD %d: PRODUCER] Produced clock: ", process_rank);
  // clock_print(&process_clock);
  // printf("\n");
}

void event_send(i32 id){
  process_clock.ps[process_rank]++;
  // TODO: create and enqueue to out

  Task t = {
    .destination = id,
    .origin = process_rank,
    .clock = process_clock
  };

  task_enqueue_multithreaded(&queue_out, t);

  printf("[THREAD %d] Sent clock to %d: ", process_rank, id);
  clock_print(&t.clock);
  printf("\n");
}

void event_receive(){
  process_clock.ps[process_rank]++;

  Task receive;
  task_dequeue_multithreaded(&queue_in, &receive);

  for (i32 i = 0; i < NUM_PROCESSES; ++i) {
    if (receive.clock.ps[i] > process_clock.ps[i]) {
      process_clock.ps[i] = receive.clock.ps[i];
    }
  }


  printf("[THREAD %d] Received clock from %d: ", process_rank, receive.origin);
  clock_print(&receive.clock);
  printf("\n");
}

void process0(void){ 
  printf("[THREAD %d] spawning process ", process_rank);
  clock_print(&process_clock);
  printf("\n");

  event_internal_process();
  event_send(1);
  event_receive();
  event_send(2);
  event_receive();
  event_send(1);
  event_internal_process();

  printf("[THREAD %d] final clock: ", process_rank);
  clock_print(&process_clock);
  printf("\n");
}
void process1(void){
  printf("[THREAD %d] spawning process ", process_rank);
  clock_print(&process_clock);
  printf("\n");

  event_send(0);
  event_receive();
  event_receive();

  printf("[THREAD %d] final clock: ", process_rank);
  clock_print(&process_clock);
  printf("\n");
}
void process2(void){
  printf("[THREAD %d] spawning process ", process_rank);
  clock_print(&process_clock);
  printf("\n");

  event_internal_process();
  event_send(0);
  event_receive();

  printf("[THREAD %d] final clock: ", process_rank);
  clock_print(&process_clock);
  printf("\n");
}


void *consumer_thread(void *data){
  for (;;) {
    event_consume();
  }
  pthread_exit(NULL);
}

void *producer_thread(void *data){
  for (;;) {
    event_produce();
  }
  pthread_exit(NULL);
}

int main(void){
  MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
  assert(process_rank < NUM_PROCESSES);

  pthread_mutex_init(&queue_in.lock, NULL);
  pthread_cond_init(&queue_in.cond_has_items, NULL);
  pthread_cond_init(&queue_in.cond_has_space, NULL);

  pthread_mutex_init(&queue_out.lock, NULL);
  pthread_cond_init(&queue_out.cond_has_space, NULL);
  pthread_cond_init(&queue_out.cond_has_items, NULL);


  pthread_t producer_thread_id;
  pthread_create(&producer_thread_id, NULL, producer_thread, NULL);

  pthread_t consumer_thread_id;
  pthread_create(&consumer_thread_id, NULL, consumer_thread, NULL);

  switch (process_rank){
  case 0: process0(); break;
  case 1: process1(); break;
  case 2: process2(); break;
  }

  MPI_Finalize();
  return 0;
}
