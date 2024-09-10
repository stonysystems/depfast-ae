```
python3 waf configure build -J
```

### Implementations
#### chaining rpc model
1. predetermine chains - control # of chains <= 10 to avoid path explosion 
2. doing a forward and backward on each path  

Path1-0: L -> F1 -> F2 -> F3 -> F4 (forward)
Path1-1: L -> F4 -> F3 -> F2 -> F1 (backward)
Path2-0: L -> ... (forward)
Path2-1: L -> ... (backward)

Traversing a path both forward and backward ensures that neither direction is affected by failed or slow nodes. We can resubmit a request to the other one if there is a failure or slow in one direction. We should return earlier if enough YES or NO are collected.


#### best efforts in-order delivery
In most cases, all paths should have similar responsiveness, with roughly equal weights. We will distribute requests across all paths, which may result in out-of-order delivery issues.


### References
1. https://github.com/kshivam26/depfast-ae/tree/micro
2. https://github.com/kshivam26/depfast-ae/compare/kshivam26:depfast-ae:7436ab8...kshivam26:depfast-ae:micro