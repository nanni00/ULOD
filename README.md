# UseLess Open Data

A very simple, unefficient, unsafe library for downloading Open Data from different endpoints.


### Warning!

Parallelization is not perfectly handled: sometimes the code stucks even if all available datasets have been already downloaded, or if you want to stop the execution a simple Ctrl+C might sadly fail, and a more drastic solution is required (e.g. close the tab, or kill processes).
