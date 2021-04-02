## Code style

We use the [Google's C++ Style Guide](https://google.github.io/styleguide/cppguide.html).

## Avoid git merge

Git merge could be painful and we try to avoid it as much as possible. Instead we use `git rebase`. 

To avoid auto merge, please use `git pull --rebase` before you push if you are working on a shared branch.
