#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h> 

#define PERMS 0644
#define SIZE 50

struct my_msg {         // this structure used when dealing with message queues
    long m_type;
    char mtext[SIZE];   // while sending holds the filename and while receiving it holds the result
};

int main()
{
    int msqid;
    key_t key;

    if((key = ftok("load_balancer.c",'B'))==-1){
        perror("Error while creating file key\n");
        exit(-1);
    }
    if((msqid=msgget(key,PERMS | IPC_CREAT)) == -1){
        perror("Error while creating message queue\n");
        exit(-2);
    }
    printf("Load Balancer is Ready\n");
    while(1)
    {    
        struct my_msg message;
        if(msgrcv(msqid, &message, SIZE,1,0 )==-1) // receives of type 1;
        {
            perror("Error in receiving msg \n");
            exit(-3);
        }
        puts("msg received - ");
        printf("%s\n",message.mtext);
        long operation_number = (int)message.mtext[3] - 48;
        long sequence_number = ((int)message.mtext[0]-48)*100 + ((int)message.mtext[1]-48)*10 +((int)message.mtext[2]-48);
        printf("%ld - seq \n",sequence_number);
        printf("%ld - op \n",operation_number);
        if(operation_number%10==5)      // cleanup
        {
            break;
        }
        if(operation_number<=2)      // primary_server; =>mtype = 4;
        {
            puts("redirected to Primary Server");
            message.m_type = 4;                    
            if(msgsnd(msqid,&message,SIZE,0)==-1)      // error in sending message;
            {
                perror("error in sending message to client");
                exit(-1);
            }
        }   
        else                            
        {
            if(sequence_number%2==1) // secondary_server 1 =>mtype = 2;
            {
                puts("redirected to Secondary Server 1");
                message.m_type = 2;            
                if(msgsnd(msqid,&message,SIZE,0)==-1)      // error in sending message;
                {
                    perror("error in sending message to client");
                    exit(-1);
                }
            }
            else // secondary_server 2 =>mtype = 3;
            {
                puts("redirected to Secondary Server 2");
                message.m_type = 3;          
                if(msgsnd(msqid,&message,SIZE,0)==-1)      // error in sending message;
                {
                    perror("error in sending message to client");
                    exit(-1);
                }
            }
        }
        puts("////////////////////////////////////////////////////////////////////////////////////////////////////");
    
    }
// CHECK THIS PART OUT;
    int wpid, status;
    while ((wpid = wait(&status)) > 0)
        ; // ensures all the child processes terminate before main server terminates
    // new code
    struct my_msg message;
    message.m_type = 3;
    message.mtext[0] = 't';
    if (msgsnd(msqid, &message, SIZE, 0) == -1)
    {
        perror("Error in sending mssage");
        exit(-1);
    }
    message.m_type = 2;
    if (msgsnd(msqid, &message, SIZE, 0) == -1)
    {
        perror("Error in sending mssage");
        exit(-1);
    }
    message.m_type = 4;
    message.mtext[3] = 53;
    if (msgsnd(msqid, &message, SIZE, 0) == -1)
    {
        perror("Error in sending mssage");
        exit(-1);
    }
    sleep(5);

    // code end
    if (msgctl(msqid, IPC_RMID, NULL) == -1) // closes the message queue
    {
        perror("Error in msgctl in line 36\n");
        exit(-4);
    }
}