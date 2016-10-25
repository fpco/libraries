## visualise the data from the particle filter benchmark.
##
## The plots are designed to show two things:
##
## - the scaling behaviour of the particle filter benchmark when more
##   slaves are added.
##
## - differences in performance between multiple commits.
##
## In order to generate the plots, perform the following steps:
##
## - build and run the benchmark for each commit you want to include
##   in the comparison.  Eacht time the benchmark is run, it will add
##   its data to a csv file (bench-pfilter.csv by default), tagged by
##   the current git commit hash.
##
##   stack exec -- wq-performace pfilter
##
##   The parameters you might want to give are:
##
##     -m and -n, to set the number of slaves used in the benchmark
##     -N, to set the number of particles
##
##   You also might want to run the benchmark a few times for each
##   commit, to get more stable numbers.
##
## - adjust the following line to match the first 8 digits of the
##   commit hash you want to compare performance against.
##
masterHash <- "2086937b"
masterText <- paste("master (", masterHash, ")")
##
## - Use R to run this file, as in
##
##   R -f work-queue/pfilter-bench-plots.r
##
## This will generate three files:
##
## - pfilter-scaling.png, showing the scaling behaviour of each commit
## - pfilter-improvements.png, showing the ratio of
##   wall-time(commit)/wall-time(master)
## - Rplots.pdf, containing both plots

## required libraries
library('ggplot2')
library('plyr')
library('tidyr')
library('data.table')

## Read in data from benchmark runs
timings <- read.csv('bench-pfilter.csv')
summary(timings)
timings$stepsize <- as.factor(timings$stepsize)
timings$deltat <- as.factor(timings$deltat)
timings$particles_as_factor<- as.factor(timings$particles)
timings$omega2 <- as.factor(timings$omega2)
timings$omega2_interval <- as.factor(timings$omega2_interval)
timings$phi0 <- as.factor(timings$phi0)
timings$resample_threshold <- as.factor(timings$resample_threshold)
levels(timings$commit)[levels(timings$commit)==masterHash] <- masterText
timings$slaveWorkFraction <- timings$spWorkWall/(timings$spWorkWall + timings$spReceiveWall + timings$spSendWall)
profiling <- gather(timings, action, t, spReceiveWall:mpSendCount
                  , na.rm = TRUE
                  , factor_key=TRUE)

## Draw log/log plot of the wall-time over the slave count.  For
## perfect 1/n scaling, this will be a straight line with slope of -1.
scalingPlot <- qplot(slaves, time/(particles/1000)/steps
                   , data=timings
                   , colour=commit
                   , alpha = I(1/3))
scalingPlot +
    ggtitle("wall-time of particle filter benchmark") +
    ylab("time[s] per iteration per 1000 particles") +
    xlab("number of slaves")+
    scale_x_log10(breaks = 1:30, minor_breaks = NULL) +
    scale_y_log10(breaks = c(.1, .2, .4, .8, 1.6, 3.2)
                , minor_breaks = (1:20)/10) +
    geom_smooth() +
    geom_smooth(data=timings,
                aes(x = slaves, y = (time/(particles/1000)/steps)*slaveWorkFraction/mean(timings[timings$slaves==1,]$slaveWorkFraction, na.rm=TRUE),
                    colour=commit),
                linetype="dashed") +
    geom_abline(intercept = log10(mean(timings[(timings$slaves==1),]$time)/100)
              , slope=-1
              , linetype="dotted")
ggsave("pfilter-scaling.png")

## Show speedup relative to master branch before optimisations.
attach(timings)
normalizedTimings <- ddply(
    merge(timings, aggregate(list(meantime=time), by=list(commit=commit, node=node, slaves=slaves, particles=particles), mean)),
    ~ node + slaves + particles,
    transform,
    Ratio=time/meantime[commit==masterText])
detach(timings)
qplot(slaves, Ratio, data=normalizedTimings[normalizedTimings$commit != masterText,], colour=commit, alpha=I(0.5)) +
    ggtitle("Performance improvement") +
    xlab("number of slaves") +
    ylab(paste("time/time(", masterText, ")")) +
    geom_smooth()
ggsave('pfilter-improvements.png')

## Show an overview of all slave profiling times
qplot(slaves, t, data=profiling[(profiling$action %like% '^sp') & !(profiling$action %like% 'Count$'),]
    , colour=commit
    , alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10() +
    geom_smooth() +
    facet_wrap( ~ action)
ggsave('pfilter-slave-times.png')

## Show an overview of all slave profiling counts
qplot(slaves, t, data=profiling[(profiling$action %like% '^sp') & (profiling$action %like% 'Count$'),]
    , colour=commit
    , alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10() +
    geom_smooth() +
    facet_wrap( ~ action)
ggsave('pfilter-slave-counts.png')

## Show an overview of all master profiling times
qplot(slaves, t, data=profiling[(profiling$action %like% '^mp') & !(profiling$action %like% 'Count$'),]
    , colour=commit
    , alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10() +
    geom_smooth() +
    facet_wrap( ~ action)
ggsave('pfilter-slave-times.png')

## Show an overview of all master profiling counts
qplot(slaves, t, data=profiling[(profiling$action %like% '^mp') & (profiling$action %like% 'Count$'),]
    , colour=commit
    , alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10() +
    geom_smooth() +
    facet_wrap( ~ action)
ggsave('pfilter-slave-counts.png')
