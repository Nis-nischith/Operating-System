#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>     // defines various constants, types, functions related to system calls -(functions) fork, exec, pipe....stdin_fileno, stdout_fileno(constants)
#include <sys/types.h>  // defines various data types used by system calls and other system related functions
#include <sys/wait.h>   // provides constants and functions when dealing with child process..wait and waitpid
#include <string.h>
#include <sys/ipc.h>    // ipc inter process communication..like message queues, shared memory
#include <sys/msg.h>    // contains data structures and functions of message queues, send receive create msg queue..helps
#include <sys/shm.h>    // header file to use shared memory...

#define PERMS 0644      // 644 means you can read and write the file or directory and other users can only read it
#define SIZE 50

struct my_msg {         // this structure used when dealing with message queues
    long m_type;
    char mtext[SIZE];   // while sending holds the filename and while receiving it holds the result
};
struct read_op{
    int source;
};
struct write_op{
    int n;
    int A[SIZE][SIZE];
};

int main()
{
    int msqid;                  // it is the identifier of the message queue that is created or accessed by my program 
    key_t key;                  // used to store the unique key that identifies the message queue
    // The key is used to create or access the message queue, while the identifier (msqid) is used to interact with the specific message queue once it has been created or opened.

    if((key = ftok("load_balancer.c",'B'))==-1){ // ftok generates a unique key for the message queue. ftok takes in a filename and project identifier(B here)
        perror("Error while creating key\n"); // if it fails to create a key, it prints an error message
        // error when file dne, file is a directory, permission denied, etc.
        exit(-1);   // exit is used to terminate a program and return a status code to OS..exit(0) =>successful, non zero exit => error or some abnormal condition
    }
    if((msqid=msgget(key,PERMS)) == -1){  
        perror("Error while creating message queue\n");
        // error when message queue DNE, may be limits on no. of message queues that can exist..invalid key, permission issues. 
        exit(-2);   
    }
    while(1)
    {
        puts("1. Add a new graph to the database");
        puts("2. Modify an existing graph of the database");
        puts("3. Perform DFS on an existing graph of the database");
        puts("4. Perform BFS on an existing graph of the database");

        long sequence_number, operation_number; char graph_filename[4];

        puts("\nEnter Sequence Number");
        scanf("%ld",&sequence_number);

        puts("Enter Operation Number");
        scanf("%ld",&operation_number);

        puts("Enter Graph File Name");
        scanf("%s",graph_filename);

        // create a shared memory segment;
        int shmid = shmget((key_t)sequence_number, sizeof(int[50][50]), 0666 | IPC_CREAT); // shmget gets an identifier in shmid, 0666 read and write for everyone..
        if(shmid == -1){
            perror("Shmid\n");
            exit(-1);
        }
        // create a struct to send message;
        struct my_msg message;
        memset(message.mtext, '\0',SIZE);
        message.m_type = 1;
        message.mtext[3] = operation_number + 48;

        message.mtext[2] = sequence_number%10 + 48;
        message.mtext[1] = (sequence_number/10)%10 +48;
        message.mtext[0] = sequence_number/100 + 48;

        strcat(message.mtext, graph_filename);
        long want = sequence_number + 5L;
        if(operation_number<=2)
        {
            // create a read struct that is to be shared in shared memory;
            struct write_op *shared_mem_write;
            shared_mem_write = (struct write_op *)shmat(shmid, NULL, 0);
            if(shared_mem_write == (struct write_op *)(-1)) 
            {
                perror("shmat");
                exit(1);
            }
            printf("Enter No of nodes :");
            scanf("%d",&shared_mem_write->n);
            printf("Enter Adjacency matrix\n");
            for(int i =0; i<shared_mem_write->n; i++)
            {
                for(int j = 0; j<shared_mem_write->n; j++)
                {
                    scanf("%d",&shared_mem_write->A[i][j]);
                }
            }

            // send
            if(msgsnd(msqid,&message,SIZE,0)==-1)  // send [msqid, struct ping, size, 0] => 0 is used as a msg_flag => set to 0 =? no additional flags or options specified.
            {
                perror("error sending request to the load balancer\n"); // print error when invalid msqid, insufficient permission, message queue full, system resource limitations
            }
            puts("....getting result from server");
            // receive
            if(msgrcv(msqid,&message,SIZE,want,0)==-1) // receive a response message from the server using the msqid. specifies the sequence_number
            {
                perror("error in getting results form server\n");// print error when invalid msqid, insufficient permission, message queue removed, system resource limitations
            }   
            printf("%s\n\n",message.mtext);      // print the received m.text => successful or not shit...
            shmdt(shared_mem_write); // detach from shared memory
        }
        else
        {
            // create a read struct that is to be shared in shared memory;
            struct read_op *shared_mem_read;
            shared_mem_read = (struct read_op *)shmat(shmid, NULL, 0);
            if(shared_mem_read == (struct read_op *)(-1)) 
            {
                perror("shmat");
                exit(1);
            }
            
            printf("Choose Initial Point to Perform Traversal:- ");
            scanf("%d",&shared_mem_read->source);

            // send
            if(msgsnd(msqid,&message,SIZE,0)==-1)  // send [msqid, struct ping, size, 0] => 0 is used as a msg_flag => set to 0 =? no additional flags or options specified.
            {
                perror("error sending request to the load balancer\n"); // print error when invalid msqid, insufficient permission, message queue full, system resource limitations
            }
            printf("....getting result from server\n");
            // receive
            if(msgrcv(msqid,&message,SIZE,want,0)==-1) // receive a response message from the server using the msqid. specifies the sequence_number
            {
                perror("error in getting results form server\n");// print error when invalid msqid, insufficient permission, message queue removed, system resource limitations
            }   
            puts("Result through Traversal are:-");
            printf("%s",message.mtext);
            printf("\n");
            shmdt(shared_mem_read); // detach from shared memory
        }
        // destroy the shared memory
        shmctl(shmid, IPC_RMID, NULL);
        puts("////////////////////////////////////////////////////////////////////////////////////////////////////\n");
    }
    return 0;
}