ALTER TABLE "jobparameter" 
  ADD CONSTRAINT set_jobid_name_key UNIQUE(jobid, name);
