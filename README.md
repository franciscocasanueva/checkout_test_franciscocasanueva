This is the solution of Francisco Casanueva to the Data Analyst Checkout technical task.

### Problem

Build the Data Warehouse tables/structures which will allow our BI tool to easily and in a performant way answer 2 questions:

- Number of pageviews, on a given time period (hour, day, month, etc), per postcode - based on the current/most recent postcode of a user.
- Number of pageviews, on a given time period (hour, day, month, etc), per postcode - based on the postcode a user was in at the time when that user made a pageview.

### Sources

***users_extract***:

Operational database containing information about users. A user has 2 properties:
- ***id​***, uniquely identifying each user. Example: ​ 1234
- ***postcode​*** , indicating where a user is at the moment. This attribute may change regularly based on the user’s location. Example: ​ SW19

Updated: daily
20M records

***pageview_extract***:

The other source consists of an operational database containing information about pageviews to a website. A pageview has 3 properties:
- ***user_id***​, uniquely identifying a user. This matches the ​ id​ on the users table. Example: ​ 1234
- ***url*** of the page being visited. Example: ​ www.website.com/index.html
- ***pageview_datetime***​ when the pageview occurred. Example: ​ 2019-10-11 14:55:23

Updated: hourly
100M records daily

### Solution

***Final table:***

We want the users to be able to query these 2 requirements with only one table, and if possible without any joins in order to achieve the best performance possible for the analytics that will be run. I achieved this with the final data model ___pageviews_postcode___:

| user_id  | pageview_datetime  | original_postcode  | current_postcode  | postcode_updated_date  |
|---|---|---|---|---|
| 1234  |  2019-10-11 14:55:23 | SW19  |  SW20 |  2019-10-20 00:00:03 |

***Model:***

We want to prevent daily loading the whole ***pageview_extract*** as it has 100M daily rows. In order to do this we use a incremental table on the final table of ___pageviews_postcode___ this incremental has 2 parts:

1. Load the new entries of ***pageview_extract*** into ___pageview_postcode___ using their last postcode from ___current_postcode___.
2. Update the postcodes that have changed since the last run of ___pageviews_postcode___. To do this we create a view with all the postcodes that have changed and need to be updated called ___updated_postcode_stg___.


***Scheduling strategy***:

In order to showcase the scheduling of the model I have implemented a 2 cronjobs that will update the tables:
  - ___current_postcode___ and ___updated_postcode_stg___ daily
  - ___pageviews_postcode___ hourly

The crontab file can be found on this repo. In order to use it on a different server the HOME variable on that file should be replaced with the path where this repo is saved. This crontab:
- Automatically pulls any changes of the repo every morning.
- Copies the crontab file from the repo to the linux crontab file every morning (this is not sustainable with more jobs, you would have to just create one version control file of the crontab).
- Executes the hourly and daily dbt runs.

In order to initiallize the model you will have to first run the models manually in the following order:
___current_postcode___
___pageviews_postcode___
___updated_postcode_stg___

This could be done in a much more sustainable way using a workflow scheduler like AirFlow.
