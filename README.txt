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

        1. TriGramMR:
            map:
                maps each line to the trigram (validate 3 words, and remove stopwords) and a group (random 1 or 0)
            reduce:
                group all trigrams (as key) and the count of each group (as a pair of ints)

        2. ParamsMR
            map:
                from each trigram we create the 4 entries (each for every param) which will be summed up to the nr0, nr1, tr01, tr10 of the whole corpus
            reduce:
                1. key: NR0, *SOME R VALUE* -> we sum the number of trigrams appeared R times in group 0 and write for each trigram its nr0 value
                2. key: NR1, *SOME R VALUE* -> we sum the number of trigrams appeared R times in group 1 and write for each trigram its nr1 value
                3. key: TR01, *SOME R VALUE* -> we sum the number of appearances in group 1 of trigrams which appeared R times in group 0 and write it as tr01 value for each trigram
                4. key: TR10, *SOME R VALUE* -> we sum the number of appearances in group 0 of trigrams which appeared R times in group 1 and write it as tr10 value for each trigram

        3. ProbCalc
            map: organizes the params to match one key (the trigram) in the reduce func
                (result of map for giver trigram t is:
                    t, (nr0, *some value*)
                    t, (nr1, *some value*)
                    t, (tr01, *some value*)
                    t, (nr10, *some value*))
             reduce:
                given a key which is the trigram, we now can finally calculate the formula according to all 4 params mentioned above.

        4. SortResults:
            map: for sorting purposes we arrange like this:
               ((word1, word2, word3), score) -> ((word1 word2, score), word3)
            reduce: now when reducing all results by key (word1 word3, score) it is converted back to the form (word1 word3 word3, score)
                    but sorted topologically by the first two words and then the score.

