### What does S3 stand for?
#### Option 1. S3(b1, p1) vs Option 2. S3(b1, on=[p1])
##### Pros for Option 1
* Looks nice for when you have to copy from prefix to prefix. e.g. `S3(b, p1) >> CopyLambda >> S3(b, p2) >> MoveLambda` vs `S3(b, on=[p1]) >> CopyLambda(dest_prefix=p2) >> S3(b, on=[p2]) >> MoveLambda` 

* Works well for multiple prefixes to different services
```python
# Option 1
p1 = S3(b, p1) >> L1
p2 = S3(b, p2) >> SQS1

# Option 2
S3(b, on=[event(p1, LAMBDA), event(p2, SQS)]) >> [L1, SQS1]
 
```
I have the unnecessary `LAMBDA` and `SQS` in my `event` function
  
##### Cons for Option 1
* Does not look nice for when you have multiple prefixes from the same bucket going to one Lambda or multiple prefixes
 going to different services e.g. A Lambda and a SQS. 
 ```python
  p1 = S3(b, p1) >> L1
  p2 = S3(b, p2) >> L1
  piper = Pipeline(p1, p2)
  #vs
  s3(b, on=[p1, p2]) >> L1
 ```
    * Two separate S3 objects for the same bucket. Doesn't convey all the notifications from that S3 bucket at a 
    glance.
    * We have to use `Pipeline` to combine both of them in the same pipeline. But I'd like to keep the Pipeline concept
     only for multiple parents to same the service. One concept, one API.
    * If I have to connect a Lambda e.g. `L0` to this pipeline say with an S3 with two prefixes. In `option 1`, I'll
        * have to connect it one of the s3 objects. This does not give a complete picture
        * have to connect `L0` to a Pipeline representing both S3 objects for each prefix? Unnecessary.  

### Other Design Alternatives
* Have an `S3` for option2 and make a new object `S3Objects` for option 1