This repo contains example python scripts for processing the reddit dump files created by pushshift. The files can be torrented from [here](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1).

* `single_file.py` decompresses and iterates over a single zst compressed file
* `iterate_folder.py` does the same, but for all files in a folder
* `combine_folder_multiprocess.py` uses separate processes to iterate over multiple files in parallel, writing lines that match the criteria passed in to text files, then combining them into a final zst compressed file