## Hyperskill data analysis

1. Merge datasets with solutions, users and code quality analysers results 
```shell
python3 analysis/src/python/hyperskill_statistics/common/build_submissions
 -s analysis/src/python/hyperskill_statistics/data/java/solutions_java11.csv 
 -u analysis/src/python/hyperskill_statistics/data/java/submission_to_user_anon.csv
 -ri analysis/src/python/hyperskill_statistics/data/java/solutions_unique_with_raw_issues_java11.csv
 -qi analysis/src/python/hyperskill_statistics/data/java/solutions_unique_with_qodana_issues_java11.csv
 -o analysis/src/python/hyperskill_statistics/data/java/submissions_with_issues_java11.csv 
```

2. Filter submissions in submissions series (grouped by user, step) which have the same code or different code 
   (possible stranger's submission)
```shell
python3 analysis/src/python/hyperskill_statistics/common/filter_submissions.py
 -s analysis/src/python/hyperskill_statistics/data/java/submissions_with_issues_java11.csv 
 -o analysis/src/python/hyperskill_statistics/data/java/submissions_filtered_with_issues_java11.csv
```

3. Using hyperskill client from analysis/src/python/data_collection load all information about steps and topics 
   mentioned in solutions
```shell
python3 analysis/src/python/data_collection/run_data_collection.py
 -o step
 -f analysis/src/python/hyperskill_statistics/data/java/submissions_filtered_with_issues_java11.csv
 -c step_id
 -out analysis/src/python/hyperskill_statistics/data/java
```

```shell
python3 analysis/src/python/data_collection/run_data_collection.py
 -o topic
 -f analysis/src/python/hyperskill_statistics/data/java/steps.csv
 -c topic
 -out analysis/src/python/hyperskill_statistics/data/java
```

4. Run preprocessing where some additional information added to dataframes:
    * topic depth
    * steps complexity/difficulty
    * users level
    * client from base client
   
4. Run statistics analysers to get required statistics

