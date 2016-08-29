## visualise the data from the particle filter benchmark.
library('ggplot2')
library('plyr')


## Read in data from benchmark runs
timings <- read.csv('bench-pfilter.csv')
timings$stepsize <- as.factor(timings$stepsize)
timings$deltat <- as.factor(timings$deltat)
timings$particles_as_factor<- as.factor(timings$particles)
timings$omega2 <- as.factor(timings$omega2)
timings$omega2_interval <- as.factor(timings$omega2_interval)
timings$phi0 <- as.factor(timings$phi0)
timings$resample_threshold <- as.factor(timings$resample_threshold)
levels(timings$commit)[levels(timings$commit)=="492133ed"] <- "master (492133ed)"
levels(timings$commit)[levels(timings$commit)=="21e644a8"] <- "less conversions (21e644a8)"
levels(timings$commit)[levels(timings$commit)=="ab89d49d"] <- "hashtables (ab89d49d)"
## We're not really interested in low slave count
timings <- timings[timings$slaves > 3,]
timings <- timings[timings$slaves < 18,]
## 10000 particles is what matches the HMST dpf best
timings <- timings[timings$particles == 10000,]
## Draw log/log plot of the wall-time.
timingsPlot <- qplot(slaves, time/(particles/1000)/steps
                   , data=timings
                   , colour=commit
                   , alpha = I(1/3))
timingsPlot +
    ggtitle("wall-time of particle filter benchmark") +
    ylab("time[s] per iteration per 1000 particles") +
    xlab("number of slaves")+
    scale_x_log10(breaks = 1:30, minor_breaks = NULL
                , limits = c(4,18)) +
    scale_y_log10(breaks = c(.1, .2, .4, .8, 1.6, 3.2)
                , minor_breaks = (1:20)/10) +
    geom_smooth()

ggsave("pfilter-scaling.png")

## Show speedup relative to master branch before optimisations.
attach(timings)
normalizedTimings <- ddply(
    merge(timings, aggregate(list(meantime=time), by=list(commit=commit, node=node, slaves=slaves, particles=particles), mean)),
    ~ node + slaves + particles,
    transform,
    Ratio=time/meantime[commit=='master (492133ed)'])
detach(timings)
qplot(slaves, Ratio, data=normalizedTimings[normalizedTimings$commit != "master (492133ed)",], colour=commit, alpha=I(0.5)) +
    ggtitle("Performance improvement") +
    xlab("number of slaves") +
    ylab("time/time(master (492133ed))") +
    geom_smooth()

ggsave('pfilter-improvements.png')
