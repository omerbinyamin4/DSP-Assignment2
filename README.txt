# Assignment1DSP

    Omer Benjamin : 312184187
    Arnon Portal : 316327766

    Output URL: s3://dsp2-hadoop/output4/part-r-00000

# How to run :
    1. Create credentials file with your AWS Lab Session credentials at this path :
        a. For Windows : C:\Users\<your_user>\.aws\credentials
        b. Mac : ~\.aws\credentials

    2. We create one Jar file that contains all classes, the only change between the steps is the main class to run.
       To initiate a job flow at the Main.java class :
       1. Select the wanted jar : ass2.jar is without local aggregation, ass2LA.jar is with the local aggregation.
       2. Make sure that the outputs folder on S3 not already exists with the same name.
       3. Run main.
       4. After the run, the output will be located at s3://dsp2-hadoop/<Step 4 (Sort) Output>/part-r-00000

# Map-Reduce Steps:


