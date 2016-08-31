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
masterHash <- "51a07603"
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


library('ggplot2')
library('plyr')

masterText <- paste("master (", masterHash, ")")

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
levels(timings$commit)[levels(timings$commit)=="21e644a8"] <- "less conversions (21e644a8)"
levels(timings$commit)[levels(timings$commit)=="ab89d49d"] <- "hashtables (ab89d49d)"
## ## If you have aggregated a lot of data and want to plot only a subset, you can filter it:
## timings <- timings[timings$slaves > 3,]
## timings <- timings[timings$slaves < 18,]
## timings <- timings[timings$particles == 10000,]

## Draw log/log plot of the wall-time.
timingsPlot <- qplot(slaves, time/(particles/1000)/steps
                   , data=timings
                   , colour=commit
                   , alpha = I(1/3))
timingsPlot +
    ggtitle("wall-time of particle filter benchmark") +
    ylab("time[s] per iteration per 1000 particles") +
    xlab("number of slaves")+
    scale_x_log10(breaks = 1:30, minor_breaks = NULL) +
    scale_y_log10(breaks = c(.1, .2, .4, .8, 1.6, 3.2)
                , minor_breaks = (1:20)/10) +
    geom_smooth() +
    geom_smooth(data=timings,
                aes(x = slaves, y = (time/(particles/1000)/steps)*slaveWorkFraction,
                    colour=commit))

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

## Show average 'productivity' of the slaves (i.e., t_work/(t_total),
## where t_work is the time spent calculating responses to requests,
## and t_total is t_work plus the time spent receiving requests and
## sending responses).
qplot(slaves, slaveWorkFraction,
      data=timings,
      colour=commit,
      alpha = I(1/3)) +
    ggtitle("average productivity of slaves") +
    labs(y=expression(t[work]/(t[work]+t[receive]+t[send])),
         x="number of slaves")+
    geom_smooth()

ggsave('pfilter-productivity.png')

