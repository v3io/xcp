# xcp
fast directory copy to/from any combination of local files, AWS S3, and iguazio v3io


## Usage

`xcp [flags] source dest`

Example:

    xcp -r -f *.ipynb v3io://webapi:8081/users/iguazio tsts8

source and destination are URLs<br>
> for faster performance (parallelism) use more workers using the `-w` flag

URL examples:
```
 s3 paths:
    s3://<bucket>/path
    s3://<access_key>:<secret_key>@<bucket>/path
    
 v3io paths:
    v3io://<API_URL>/<container>/<path>
    v3io://<username>:<password>@<API_URL>/<container>/<path>
    v3io://:<session_key>@<API_URL>/<container>/<path>

 local paths:
    path/to/files
    /opt/xyz
    c:\windows\path
```

#### Flags
```
  -f string
        filter string e.g. *.png
  -hidden
        include hidden files (start with '.')
  -m int
        maximum file size
  -n int
        minimum file size
  -r    
        Recursive (go over child dirs)
  -t string
        minimal file time e.g. 'now-7d' or RFC3339 date
  -v string
        log level: info | debug (default "debug")
  -w int
        num of worker routines (default 1)
```
