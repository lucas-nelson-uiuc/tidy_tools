# Roadmap

There are a couple directions I would like to take Tidy Tools given the time.
Although I'm happy making these changes even if it just benefits myself, any
input from the community would be greatly appreciated! I will try my best to
update this roadmap as the project continues to evolve.

## Observability and Logging Support

As of writing, `TidyDataFrame` and `TidyDataModel` both include logging
functionality. Although these features are helpful, they do not yet feel
complete. To feel complete, Tidy Tools would need:

- an extensive `exceptions` and `logging` layer
- support across the whole package
- (ideally) parsing for logging analytics

Thankfully, `loguru` simplifies a lot of the logging headaches that I would have
to address otherwise. I'll just need to find the time to incorporate these ideas.

---

## Orchestration Layer

Building on the observability goal, I'd like to offer a module that can sit on
top of Tidy Tools to facilitate the execution of tidy workflows. I think
`TidyDataModel` includes some preliminary thoughts on this, but there is more
that can be included.

---

## Support for Other DataFrame Libraries

PySpark was an easy first target:

- It's a popular language.
- It's the language of choice at my workplace.
- It's built on Java so it won't always follow pythonic conventions.

However, I would like to extend this to other libraries. There are other packages
like `pandera` and `patito` out there already, and I don't want to rewrite the work
these projects have already done. If not for wide-adoption, it'd at least be a
great learning experience.

---

## The Inevitable Re-write

I've tried my best to develop code that will last. However, requirements and
preferences change over time, and Tidy Tools might not look the same now as
it will a couple dozen commits from now. Although I will try to delay this as
much as possible (especially if I'm the only one using this), my main reasons
as of writing are:

- **Inexperience**: I'd describe myself as a strong intermediate Pythonista and
nothing more. Aside from taking CS 105 and the many semesters teaching the class,
I'm going off what I've taught myself.
- **Rust**: This would be a perfect excuse for picking up the language.
- **Community Support**: Although I've written and thought up most of this project,
that doesn't need to always be the case. I'd be happy to learn from others and
integrate those changes to make Tidy Tools an even more impactful project.
