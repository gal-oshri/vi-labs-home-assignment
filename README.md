# Vi lab home assignemt
- I created a pyspark script solving the 4 objectives of the assignment. The Glue job is called - 'gal-oshri-home-assignment'
- I also tried to deploy the spark job trough CloudFormation but failed due to permissions error so I couldn't fully complete the task. I added the .yaml file with the configurations template called 'glue_job_properties.yaml'. The CloudFormation stack in the UI called - 'Gal-Oshri-Home-Assignment-Stack'
- I created a new glue catalog db called - 'home-assignment-gal-db' with 4 tables holding the data of the 4 objectives.
- The location of the results csv files is under - 's3://aws-glue-gal-osh-home-assignment/objectives/', there are 4 sub-folders for each objective.
