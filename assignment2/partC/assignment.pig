register s3n://hw2files/myudfs.jar
raw = LOAD  's3n://hw2storage/*' USING TextLoader as (line:chararray);
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray); 
subject_groups = group ntriples by subject;
subject_frequencies = foreach subject_groups generate group, COUNT(ntriples) as out_degree;
out_degree_groups = group subject_frequencies by out_degree;
out_degree_sequence = foreach out_degree_groups generate group, COUNT(subject_frequencies);
store out_degree_sequence into '/user/user2/out_degree_sequence' using PigStorage();

