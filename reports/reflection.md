# Reflection

## Remarks about the Project

As this is a personal reflection file, the usage of scientific neutral language or the collective “we” is not strictly followed. For a better review flow, the commits are atomic and have conventional commit messages. Another feature to improve the review flow is that the outputs of the scripts are also saved into files called `reports/partX_logs.md`, where X is the number of the team. As this is a data pipeline project, it uses the flat configuration and contains main functions that have no strict separation of concerns, however, they follow a clear chronological structure following the tasks of the assignment. Where reasonable, there is encapsulation within this structure. The project was initialised with `uv init --bare`. This is in contrast to the [SpeechSense group project](https://github.com/SpeechSense/SpeechSense), where I set up the project with a `uv` cookie cutter template. The reason is that there, we built a software project as a team, so it was helpful to have a package like structure with a source folder, automated tests pre-commit and on GitHub and other features that facilitate seamless team collaboration.

## Hardest Problem

The hardest problem in this project was to deal with the multithreading, because the threads run in parallel and share memory. So a simple sleep time of 0.6 sec (60 seconds / 100) does not work as rate limiter, because it would still be possible to start fetching data with all 10 requests at the same time. I will explain with pizza and athletes instead of API calls and threads:  
Imagine I was assigned a large number of athletes at an event and I have to make sure that they maximally grab 100 pieces of pizza per minute. So I set up 100 stop watches so that they always reset their countdown to one minute. Whenever an athlete wants to grab a piece of pizza, they have to get a stopwatch first. When they got a pizza, they start the stop watch and join the line of athletes with a stop watch. And as soon as the watch hits 0, they leave the line and return the watch to a pile. Now the hard problem is if there are no watches left in that pile: Only one athlete at a time is allowed to check if there is a free stop watch. The athlete who wants to get pizza next has to go to the front of the line and check the number on that athlete's watch. (The front of the line obtained their watch the longest time ago, so they have the smallest number.) Now the potential next athlete knows how long it takes until the oldest watch is freed up and will sleep for that amount of time until they check again for a free watch.
That said, we can go back to the language of threads and calls: There is a list of timestamps that gets rid of expired timestamps via list comprehension. When only one athlete is allowed to check if there is a free watch, it means that a thread obtains a lock and will sleep until the oldest timestamp expires before it calls the API.

## Multithreading

Multithreading made the API download faster, because multiple calls could be made at the same time. However, it also introduced risks I had to explicitly handle: race conditions on the shared rate-limiter state and the log files required locks, and using `as_completed()` meant rows arrived in completion order rather than submission order, making the output non-deterministic across runs.

## Pandas

Pandas made it easier to examine and clean the data by hand because it provides interactive data inspection, quick visualisation of missing values, easy type coercion, and immediate feedback on a small subset of the data. The sample of 50 rows immediately revealed the messy symbol formats and negative volume values that would have been invisible in a full 10,000-row scan.

## Spark

Spark helped to deal with big data from within the familiar python environment. I had to install Java first, but it enabled me to use SQL queries and process a relatively large amount of data very quickly. An additional benefit of Spark running on top of the JVM is that it integrates with the Hadoop ecosystem, meaning the same code could be pointed at the Hadoop Distributed File System or cloud storage to process seriously big data that does not fit on a single machine. This enabled analysis over the full dataset, which can be used to make business decisions. I would not be wise to make decisions that could potentially ruin a company based on manual data exploration.

## Limitations / Possible Enhancements

This project does not have any type annotations, as the setup was intentionally kept minimal in contrast to the [SpeechSense group project](thttps://github.com/SpeechSense/SpeechSense). However, type annotations would make the pipeline more robust and would make the tests implicitly stronger, because the types would be part of the contract. Also, there is a duplication of the download logic. If this project used modules and would import them, the multithreaded download logic could be imported like the `save_dictionary_to_csv` script that was provided or the `io_utils` that I added to save the console prints to a file.
