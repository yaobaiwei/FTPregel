# FTPregel: Lightweight Fault Tolerance for Pregel+

This is an extension of Pregel+ to support fault tolerance. In order to support automatic failure recovery, we require users to deploy ULFM which provides new MPI functions for failure detection and notification. While our heavyweight algorithms can support graph mutation, current prototypes remove the graph mutation functionality of Pregel+ for ease of implementation. The main proposal of this work is a lightweight checkpointing method, where only vertex states are written to a checkpoint and messages are generated from vertex states. We also extend the systems to support log-based fast recovery proposed by this paper.

To illustrate the benefit of lightweight checkpointing, we report the performance of computing PageRank on the WebUK dataset. While one superstep of computaton takes around 30 seconds, writing a conventional checkpoint takes around 60 seconds; on the other hand, it takes only around 2 seconds to write a lightweight checkpoint.

## Project Website
[http://www.cse.cuhk.edu.hk/pregelplus/ft.html](http://www.cse.cuhk.edu.hk/pregelplus/ft.html)

## License

Copyright 2018 Husky Data Lab, CUHK
