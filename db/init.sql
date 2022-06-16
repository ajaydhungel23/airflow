CREATE DATABASE info;

USE info; 

CREATE TABLE infotable
( 
    dag_id varchar(40) not null,
    task_id varchar(40) not null,
    provides_context boolean 
);

INSERT into infotable (dag_id,task_id,provides_context) VALUES ("my_dag","first_function",TRUE); 