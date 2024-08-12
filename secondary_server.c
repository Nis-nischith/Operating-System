#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>    // defines various constants, types, functions related to system calls -(functions) fork, exec, pipe....stdin_fileno, stdout_fileno(constants)
#include <sys/types.h> // defines various data types used by system calls and other system related functions
#include <sys/wait.h>  // provides constants and functions when dealing with child process..wait and waitpid
#include <string.h>
#include <sys/ipc.h> // ipc inter process communication..like message queues, shared memory
#include <sys/msg.h> // contains data structures and functions of message queues, send receive create msg queue..helps
#include <sys/shm.h> // header file to use shared memory...
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>

#define PERMS 0644 // 644 means you can read and write the file or directory and other users can only read it
#define SIZE 50

int msqid; // it is the identifier of the message queue that is created or accessed by my program
key_t key; // used to store the unique key that identifies the message queue
// The key is used to create or access the message queue, while the identifier (msqid) is used to interact with the specific message queue once it has been created or opened.

struct my_msg
{                     // this structure used when dealing with message queues
    long m_type;      // operation to perform;
    char mtext[SIZE]; // while sending holds the filename and while receiving it holds the result
};
struct read_op
{
    int source;
};
struct Queue
{
    int front, rear;
    int array[1000];
};

struct scheduler
{
    int readers;
    int writersHash[150];
};
int first_writer(struct scheduler *sch)
{
    int min = 1000000;
    for(int i=0; i<150; i++)
    {
        if(sch->writersHash) return i;
    }
    return min;
}
struct graph
{
    int n;
    int A[30][30];
    int cur;
    int par;
    char *graph_output;
    struct Queue *graph_queue;
    sem_t *mutex;
};
// function to create a queue of given capacity. It initializes size of queue as 0
struct Queue* createQueue()
{
    struct Queue* queue = (struct Queue*)malloc(sizeof(struct Queue));
    // This is important, see the enqueue
    queue->front =0;
    queue->rear=-1;
    return queue;
}

void enqueue(struct Queue* queue, int item)
{
    queue->rear++;
    queue->array[queue->rear]=item;
}
// Function to remove an item from queue. It changes front and size.
int dequeue(struct Queue* queue)
{
    if (queue->rear >= queue->front)
        queue->front++;
    else
        printf("error");
}
// Function to get front of queue
int front(struct Queue* queue)
{
    return queue->array[queue->front];
}

int size(struct Queue* queue)
{
    return queue->rear - queue->front + 1;
}
void *dfs(void *arg)
{
    struct graph this_graph;
    this_graph.n = ((struct graph *)arg)->n;
    this_graph.cur = ((struct graph *)arg)->cur;
    this_graph.par = ((struct graph *)arg)->par;
    
    for (int i = 0; i < this_graph.n; i++)
    {
        for (int j = 0; j < this_graph.n; j++)
        {
            this_graph.A[i][j] = ((struct graph *)arg)->A[i][j];
        }
    }
    int child = 0;
    for (int i = 0; i < this_graph.n; i++)
    {
        if (this_graph.A[this_graph.cur][i] == 1 && i != this_graph.par)
        {
            child++;
        }
    }
    pthread_t all_child[child];
    struct graph child_graph[child];
    child = 0;
    for (int i = 0; i < this_graph.n; i++)
    {
        if (this_graph.A[this_graph.cur][i] == 1 && i != this_graph.par)
        {
            child_graph[child].n = this_graph.n;
            for (int i = 0; i < this_graph.n; i++)
            {
                for (int j = 0; j < this_graph.n; j++)
                    child_graph[child].A[i][j] = this_graph.A[i][j];
            }
            child_graph[child].cur = i;
            child_graph[child].par = this_graph.cur;
            
            child_graph[child].graph_output = ((struct graph *)arg)->graph_output;
            // printf("new cur is %d, new par is %d\n",child_graph[child].cur, child_graph[child].par);
            int rc = pthread_create(&all_child[child], NULL, dfs, &child_graph[child]);
            if (rc)
            {
                printf("Error in creating thread %d\n", this_graph.cur);
                exit(-1);
            }
            child++;
        }
    }
    if (child == 0)
    {
        char number[5];
        sprintf(number, "%d ", this_graph.cur+1);
        strcat(((struct graph *)arg)->graph_output, number);
    } 
    else
    {
        for (int j = 0; j < child; j++)
        {
            pthread_join(all_child[j], NULL); 
        }
    }
}
void *bfs(void *arg)
{
    int n = ((struct graph *)arg)->n;
    int cur = ((struct graph *)arg)->cur;
    int this_graph[n][n];
    for (int i = 0; i < n; i++)
    {
        for (int j = 0; j < n; j++)
        {
            this_graph[i][j] = ((struct graph *)arg)->A[i][j];
        }
    }
    for(int i=0; i<n; i++)
    {
        if (this_graph[cur][i] == 1)
        {
            // sem wait
            if (sem_wait(((struct graph *)arg)->mutex))
            {
                puts("Error in creating thread");
                exit(-1);
            }
            enqueue(((struct graph *)arg)->graph_queue,i);
            // printf("%d enqueued into q\n",i);
            if (sem_post(((struct graph *)arg)->mutex))
            {
                puts("Error in creating thread");
                exit(-1);
            }
            // sem post
        }
    }
}
void *bfs_first(void *arg)
{
    struct graph this_graph;
    this_graph.n = ((struct graph *)arg)->n;
    this_graph.cur = ((struct graph *)arg)->cur;
    for (int i = 0; i < this_graph.n; i++)
    {
        for (int j = 0; j < this_graph.n; j++)
        {
            this_graph.A[i][j] = ((struct graph *)arg)->A[i][j];
        }
    }
    struct Queue* queue = createQueue();
    sem_t mutex;
    // mutex,  ((0=>thread, 1=>processes)), 1=>initial value;
    if (sem_init(&mutex, 0, 1))
    {
        puts("Error in creating thread");
        exit(-1);
    }
    this_graph.mutex = &mutex;
    enqueue(queue, this_graph.cur);
    
    int vis[31];
    for(int i = 0; i<31; i++) vis[i] = 0;

    while(size(queue)!=0)
    {
        int children_count = size(queue);
        pthread_t all_child[children_count];
        struct graph child_graph[children_count];
        int threads_created = 0;

        for(int i=0; i< children_count; i++)
        {
            if (sem_wait(&mutex))
            {
                puts("Error in creating thread");
                exit(-1);
            }
            int number = front(queue);
            dequeue(queue);
            // printf("%d dequeued\n",number);
            if (sem_post(&mutex))
            {
                puts("Error in creating thread");
                exit(-1);
            }
            if(vis[number]) continue;
            vis[number]++;

            char node[5];
            sprintf(node, "%d ", number + 1);
            strcat(((struct graph *)arg)->graph_output, node);

            child_graph[threads_created].n = ((struct graph *)arg)->n;
            child_graph[threads_created].cur = number;
            child_graph[threads_created].graph_queue = queue;
            child_graph[threads_created].mutex = &mutex;    
            for (int p = 0; p < this_graph.n; p++)
                {
                    for (int q = 0; q < this_graph.n; q++)
                    {
                        child_graph[threads_created].A[p][q] = ((struct graph *)arg)->A[p][q];
                    }
                }
            int rc = pthread_create(&all_child[threads_created], NULL, bfs, &child_graph[threads_created]);
            if (rc)
            {
                printf("Error in creating thread %d\n", this_graph.cur);
                exit(-1);
            }
            threads_created++;
        }
        for(int i=0; i<threads_created; i++)
        {
            pthread_join(all_child[i], NULL);
        }
    }
    if (sem_destroy(&mutex))
    {
        puts("Error in creating thread");
        exit(-1);
    }
}
void *read_file_thread(void *arg)
{
    puts("inside read_file_thread and reading file");
    struct my_msg message;
    message.m_type = ((struct my_msg *)arg)->m_type;
    strcpy(message.mtext, ((struct my_msg *)arg)->mtext);

    // retrieve operation_number, sequence_number, filename from => message.mtext ;
    long operation_number = (int)message.mtext[3] - 48;
    long sequence_number = ((int)message.mtext[0] - 48) * 100 + ((int)message.mtext[1] - 48) * 10 + ((int)message.mtext[2] - 48);
    char filename[4];
    strncpy(filename, message.mtext + 4, 3);

    // connect to shared memory;
    int shmid = shmget((key_t)sequence_number, sizeof(int[50][50]), 0666 | IPC_CREAT); // shmget gets an identifier in shmid, 0666 read and write for everyone..
    if(shmid == -1){
        perror("Shmid\n");
    }
    struct read_op *shared_mem_read = (struct read_op *)shmat(shmid, NULL, 0);
    if (shared_mem_read == (struct read_op *)(-1))
    {
        perror("shmat");
        exit(1);
    }
    int source = shared_mem_read->source;
    // Create a Struct which will be sent to my function;
    struct graph this_graph;
    this_graph.cur = source - 1;
    this_graph.par = -1;
    char result[150];
    puts("starting reading file");
    // read file;
    pthread_t a_thread; // thread declaration;
    FILE *file;
    file = fopen(filename, "r");
    //  scheduler entry
    key_t key1;
    int shmid1;
    if((key1 = ftok(filename, 'C')) == -1)
    {
        perror("ftok");
        exit(-1);
    }
    if((shmid1 = shmget(key1, 2048, PERMS| IPC_CREAT)) == -1)
    {
        perror("shmget");
        exit(-1);
    }
    // creating shared memory scheduler;
    struct scheduler *shared_mem_scheduler = (struct scheduler *)shmat(shmid1, NULL, 0);
    // creating semaphores for this file;
    char semaphore1[150], semaphore2[150];
    strcat(semaphore1, filename);
    strcat(semaphore2, filename);
    strcat(semaphore1, "w");
    strcat(semaphore2, "m");
    sem_t *w, *m;
    if((w = sem_open(semaphore1, O_CREAT, PERMS, 1)) == SEM_FAILED)
    {
        perror("sem_open");
        exit(-1);
    }
    if((m = sem_open(semaphore2, O_CREAT, PERMS, 1)) == SEM_FAILED)
    {
        perror("sem_open");
        exit(-1);
    }
    ///////////////////////////////////////////////////////////////
    //sem_wait(m);
    shared_mem_scheduler->readers++;
    if(shared_mem_scheduler->readers==1 || first_writer(shared_mem_scheduler) > sequence_number)
    {
        sem_wait(w);
    }
    // sem_post(m);
    ///// READ;
    if (file == NULL)
    {
        printf("Error opening file.\n");
        exit(-1);
    }
    // Read the no of nodes;
    if (fscanf(file, "%d", &this_graph.n) != 1)
    {
        printf("Error reading the number of elements.\n");
        fclose(file);
        exit(-1);
    }
    // Read graph;
    for (int i = 0; i < this_graph.n; i++)
    {
        for (int j = 0; j < this_graph.n; j++)
        {
            int num;
            if (fscanf(file, "%d", &num) == 1)
            {
                this_graph.A[i][j] = num;
            }
            else
            {
                printf("Error reading element %d.\n", i + 1);
                fclose(file);
                exit(-1);
            }
        }
    }
    // close file;
    fclose(file);
    //sem_wait(m);
    shared_mem_scheduler->readers--;
    if(shared_mem_scheduler->readers==0 || first_writer(shared_mem_scheduler) < sequence_number)
    {
        sem_post(w);
    }
    //sem_post(m);

    puts("going in bfs/dfs function");
    // run dfs/bfs;
    this_graph.graph_output = result;
    int rc;
    if (operation_number == 3)
        rc = pthread_create(&a_thread, NULL, dfs, &this_graph);
    else
        rc = pthread_create(&a_thread, NULL, bfs_first, &this_graph);
    if (rc)
    {
        puts("Error in creating thread");
        exit(-1);
    }
    pthread_join(a_thread, NULL); // waits for thread to finish;

    // send msg back directly to client;
    message.m_type = sequence_number + 5L;
    strcpy(message.mtext, result);
    if (msgsnd(msqid, &message, SIZE, 0) == -1) // error in sending message;
    {
        perror("error in sending message to client");
        exit(-1);
    }
    puts("\nInformed client about output.");
    // deattach shm;
    shmdt(shared_mem_read);
    shmdt(shared_mem_scheduler);
    // unlink semaphore
    sem_unlink(semaphore1);
    sem_unlink(semaphore2);
    // thread deleted
}
pthread_t threads[10000];
int thread_cnt = 0;
int main()
{
    int type;
    scanf("%d", &type);
    if ((key = ftok("load_balancer.c", 'B')) == -1)
    {                                         // ftok generates a unique key for the message queue. ftok takes in a filename and project identifier(B here)
        perror("Error while creating key\n"); // if it fails to create a key, it prints an error message
        // error when file dne, file is a directory, permission denied, etc.
        exit(-1); // exit is used to terminate a program and return a status code to OS..exit(0) =>successful, non zero exit => error or some abnormal condition
    }
    if ((msqid = msgget(key, PERMS)) == -1)
    {
        perror("Error while creating message queue\n");
        exit(-2);
    }
    if (type == 1) // secondary server 1;
    {
        puts("Secondary Server 1 instance created => odd seq write ops");
        while (1)
        {
            struct my_msg message;
            if (msgrcv(msqid, &message, SIZE, 2, 0) == -1) // receive a response message from the server using the msqid. specifies the sequence_number
            {
                perror("error in getting results form server"); // print error when invalid msqid, insufficient permission, message queue removed, system resource limitations
            }
            puts("received a message from load balancer of read operation in secondary server 1.\n");

            pthread_t a_thread; // thread declaration;
            int rc;
            rc = pthread_create(&a_thread, NULL, read_file_thread, (void *)&message);
            if (rc)
            {
                puts("Error in creating thread");
                exit(-1);
            }
            else
            {
                thread_cnt++;
                threads[thread_cnt] = a_thread;
            }
        }
    }
    else // secondary server 2;
    {
        puts("Secondary Server 2 instance created => even seq write ops");
        while (1)
        {
            struct my_msg message;
            if (msgrcv(msqid, &message, SIZE, 3, 0) == -1) // receive a response message from the server using the msqid. specifies the sequence_number
            {
                perror("error in getting results form server"); // print error when invalid msqid, insufficient permission, message queue removed, system resource limitations
            }
            puts("received a message from load balancer of read operation in secondary server 2.\n");
            pthread_t a_thread; // thread declaration;
            int rc;
            rc = pthread_create(&a_thread, NULL, read_file_thread, (void *)&message);
            if (rc)
            {
                puts("Error in creating thread");
                exit(-1);
            }
            else
            {
                thread_cnt++;
                threads[thread_cnt] = a_thread;
            }
        }
    }
    for (int i = 0; i <= thread_cnt; i++)
    {
        pthread_join(threads[i], NULL);
    }
    // code ends.........................................................
    return 0;
}