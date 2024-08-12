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
struct write_op
{
    int n;
    int A[SIZE][SIZE];
};
struct scheduler
{
    int readers;
    int writersHash[150];
};
void *thread_function_create(void *arg)
{
    puts("...inside thread_functon_create");
    struct my_msg message;
    message.m_type = ((struct my_msg *)arg)->m_type;
    strcpy(message.mtext, ((struct my_msg *)arg)->mtext);
    // connect to shared memory;
    long operation_number = (int)message.mtext[3] - 48;
    long sequence_number = ((int)message.mtext[0] - 48) * 100 + ((int)message.mtext[1] - 48) * 10 + ((int)message.mtext[2] - 48);
    int shmid = shmget((key_t)sequence_number, sizeof(int[50][50]), 0666 | IPC_CREAT); // shmget gets an identifier in shmid, 0666 read and write for everyone..
    if(shmid == -1){
        perror("Shmid\n");
    }
    struct write_op *shared_mem_read = (struct write_op *)shmat(shmid, NULL, 0);
    if (shared_mem_read == (struct write_op *)(-1))
    {
        perror("shmat");
        exit(1);
    }
    puts("...connected to shared mem");
    // create file;
    char filename[4];
    strncpy(filename, message.mtext + 4, 3);
    filename[3] = '\0'; // Null-terminate the string

    // Open the file for writing
    FILE *file = fopen(filename, "w");
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
    // creating shared memory scheduler and initialising it;
    struct scheduler *shared_mem_scheduler = (struct scheduler *)shmat(shmid1, NULL, 0);
    shared_mem_scheduler->readers = 0;
    for(int i=0; i<150; i++)
    {
        shared_mem_scheduler->writersHash[i] = 0;
    }
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
    // add writer;
    shared_mem_scheduler->writersHash[sequence_number]++;
    // lock file;
    sem_wait(w);
    // sleep(30);
    if (file == NULL)
    {
        printf("Unable to open the file for writing.\n");
        exit(-1);
    }

    // Write the number of vertices to the file
    fprintf(file, "%d\n", shared_mem_read->n);

    // Write the adjacency matrix to the file
    for (int i = 0; i < shared_mem_read->n; i++)
    {
        for (int j = 0; j < shared_mem_read->n; j++)
        {
            fprintf(file, "%d ", shared_mem_read->A[i][j]);
        }
        fprintf(file, "\n");
    }

    // remove writer;
    shared_mem_scheduler->writersHash[sequence_number]--;
    // Close the file
    fclose(file);
    // remove lock;
    sem_post(w);
    
    // send msg back directly to client successful or not
    message.m_type = sequence_number+5L;
    strcpy(message.mtext, "File Made successfully");
    printf("%s created, trying to send a message to client", filename);
    if (msgsnd(msqid, &message, SIZE, 0) == -1) // error in sending message;
    {
        perror("error in sending message to client");
        exit(-1);
    }
    puts("\nMessage sent, thread deleted\n");
    puts("////////////////////////////////////////////////////////////////////////////////////////////////////");
    // deattach shm;
    shmdt(shared_mem_read);
    shmdt(shared_mem_scheduler);
    // unlink semaphore
    sem_unlink(semaphore1);
    sem_unlink(semaphore2);
    // thread deleted
}
void *thread_function_modify(void *arg)
{
    puts("...inside thread_functon_modify");
    struct my_msg message;
    message.m_type = ((struct my_msg *)arg)->m_type;
    strcpy(message.mtext, ((struct my_msg *)arg)->mtext);
    // connect to shared memory;
    long operation_number = (int)message.mtext[3] - 48;
    long sequence_number = ((int)message.mtext[0] - 48) * 100 + ((int)message.mtext[1] - 48) * 10 + ((int)message.mtext[2] - 48);
    int shmid = shmget((key_t)sequence_number, sizeof(int[50][50]), 0666 | IPC_CREAT); // shmget gets an identifier in shmid, 0666 read and write for everyone..
    if(shmid == -1){
    perror("Shmid\n");
    }
    struct write_op *shared_mem_read = (struct write_op *)shmat(shmid, NULL, 0);
    if (shared_mem_read == (struct write_op *)(-1))
    {
        perror("shmat");
        exit(1);
    }
    puts("...connected to shared mem");
    // create file;
    char filename[4];
    strncpy(filename, message.mtext + 4, 3);
    filename[3] = '\0'; // Null-terminate the string

    // Open the file for writing
    FILE *file = fopen(filename, "w");
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
    // add writer;
    shared_mem_scheduler->writersHash[sequence_number]++;
    // lock file;
    sem_wait(w);
    // sleep(40);
    if (file == NULL)
    {
        printf("Unable to open the file for writing.\n");
        exit(-1);
    }

    // Write the number of vertices to the file
    fprintf(file, "%d\n", shared_mem_read->n);

    // Write the adjacency matrix to the file
    for (int i = 0; i < shared_mem_read->n; i++)
    {
        for (int j = 0; j < shared_mem_read->n; j++)
        {
            fprintf(file, "%d ", shared_mem_read->A[i][j]);
        }
        fprintf(file, "\n");
    }

    // remove writer;
    shared_mem_scheduler->writersHash[sequence_number]--;
    // Close the file
    fclose(file);
    // remove lock;
    sem_post(w);

    /// send msg back directly to client successful or not
    message.m_type = sequence_number+5L;
    strcpy(message.mtext, "File Updated successfully");
    printf("%s updated, trying to send a message to client", filename);
    if (msgsnd(msqid, &message, SIZE, 0) == -1) // error in sending message;
    {
        perror("error in sending message to client");
        exit(-1);
    }
    puts("\nMessage sent, thread deleted\n");
    puts("////////////////////////////////////////////////////////////////////////////////////////////////////");
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
    puts("Primary Server is ready to operate !!");
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
    while (1)
    {
        struct my_msg message;
        if (msgrcv(msqid, &message, SIZE, 4, 0) == -1) // receive a response message from the server using the msqid. specifies the sequence_number
        {
            perror("error in getting results form server"); // print error when invalid msqid, insufficient permission, message queue removed, system resource limitations
        }
        long operation_number = (int)message.mtext[3] - 48;
        long sequence_number = ((int)message.mtext[0] - 48) * 100 + ((int)message.mtext[1] - 48) * 10 + ((int)message.mtext[2] - 48);
        puts("...received a message from load balancer of write operation");
        
        pthread_t a_thread; // thread declaration;
        int rc;
        if (operation_number == 1)     // create;
        {
            rc = pthread_create(&a_thread, NULL, thread_function_create, (void *)&message);
            if (rc)
            {
                puts("Error in creating thread");
                exit(-1);
            }
            thread_cnt++;
            threads[thread_cnt] = a_thread;
        }
        else if(operation_number == 2) // modify
        {
            rc = pthread_create(&a_thread, NULL, thread_function_modify, (void *)&message);
            if (rc)
            {
                puts("Error in creating thread");
                exit(-1);
            }
            thread_cnt++;
            threads[thread_cnt] = a_thread;
        }
        else if (message.mtext[0] == 't') // cleanup
        {
            for (int i = 0; i < 10000; i++)
            {
                pthread_join(threads[i], NULL); 
            }
            return 0;
        }
        if (rc)
        {
            puts("Error in creating thread");
            exit(-1);
        }
    }
}