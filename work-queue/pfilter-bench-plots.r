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
masterHash <- "97a909d0"
masterText <- paste("master (", masterHash, ")", sep="")
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
library('grid')
require('dplyr')

## Read in data from benchmark runs
timings <- read.csv('bench-pfilter.csv')
summary(timings)
timings$stepsize <- as.factor(timings$stepsize)
timings$deltat <- as.factor(timings$deltat)
timings$particles_as_factor <- as.factor(timings$particles)
timings$omega2 <- as.factor(timings$omega2)
timings$omega2_interval <- as.factor(timings$omega2_interval)
timings$phi0 <- as.factor(timings$phi0)
timings$resample_threshold <- as.factor(timings$resample_threshold)
levels(timings$commit)[levels(timings$commit)==masterHash] <- masterText
timings$slaveWorkFraction <- timings$spWorkWall / (timings$spWorkWall + timings$spReceiveWall + timings$spSendWall)

sequentialTime0 <- mean(timings[timings$slaves==0,]$time - timings[timings$slaves==0,]$mpUpdateWall)
updateTime0 <- mean(timings[timings$slaves==0,]$mpUpdateWall)

sequentialTime1 <- mean(timings[timings$slaves==1,]$time - timings[timings$slaves==1,]$mpUpdateWall)
updateTime1 <- mean(timings[timings$slaves==1,]$mpUpdateWall)

timingsToPlot <- timings

pdf("pfilter-bench-plots.pdf")

## Draw log/log plot of the wall-time over the slave count.  For
## perfect 1/n scaling, this will be a straight line with slope of -1.
ggplot(data=timingsToPlot, aes(x = slaves, y = sequentialTime0 + updateTime0/slaves, colour="Perfect scaling, 0 slaves, Amdahl"), linetype="dashed") +
    geom_smooth(linetype="dashed") +
    geom_smooth(data=timingsToPlot, aes(x = slaves, y = sequentialTime1 + updateTime1/slaves, colour="Perfect scaling, 1 slave, Amdahl"), linetype="dashed") +
    geom_smooth(data=timingsToPlot, aes(x = slaves, y = (sequentialTime0 + updateTime0) / slaves, colour="Perfect scaling, 0 slaves"), linetype="dotted") +
    geom_smooth(data=timingsToPlot, aes(x = slaves, y = (sequentialTime1 + updateTime1) / slaves, colour="Perfect scaling, 1 slave"), linetype="dotted") +
    ggtitle("wall-time of particle filter benchmark") +
    ylab("total time") +
    xlab("number of slaves") +
    geom_smooth(data=timingsToPlot, aes(x = slaves, y = time, colour=commit), alpha = I(1/3)) +
    geom_point(data=timingsToPlot, aes(x = slaves, y = time, colour=commit), alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10()

## Draw plot of the error between a perfect run and the actual run
getSequentialTime <- function(slaves, commit) {
    mean(timings[timings$slaves==1 & timings$commit==commit,]$time - timings[timings$slaves==1 & timings$commit==commit,]$mpUpdateWall)
}
getUpdateTime <- function(slaves, commit) {
    mean(timings[timings$slaves==1 & timings$commit==commit,]$mpUpdateWall)
}
getTotalTime <- function(slaves, commit) {
    mean(timings[timings$slaves==1 & timings$commit==commit,]$time)
}
perfectTime <- function(slaves, commit) {
    getSequentialTime(slaves, commit) + getUpdateTime(slaves, commit) / slaves
}
qplot(
    slaves,
    100 * time / perfectTime(slaves, commit),
    data=timings[timings$slaves > 0,],
    colour=commit) +
    ylab("How much slower we are compared to perfect scaling, 1 slave, Amdahl, in percent") +
    geom_smooth()

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

measurements <- list(
    "spReceive",
    "spWork",
    "spSend",
    "spStatefulUpdate",
    "spUpdate",
    "spReceiveInit",
    "spReceiveResetState",
    "spReceiveAddStates",
    "spReceiveRemoveStates",
    "spReceiveUpdate",
    "spReceiveGetStates",
    "spReceiveGetProfile",
    "spReceiveQuit",
    "mpUpdate",
    "mpUpdateSlaves",
    "mpRegisterSlaves",
    "mpInitializeSlaves",
    "mpGetSlaveConnection",
    "mpGetSlave",
    "mpWait",
    "mpReceive",
    "mpUpdateState",
    "mpUpdateOutputs",
    "mpSend")
wallAndCPUs <- gather(
    timings, action, t,
    one_of(unlist(lapply(measurements, function(s) { list(paste(s, "Wall", sep=""), paste(s, "CPU", sep="")) }))),
    na.rm = TRUE, factor_key = TRUE)
counts <- gather(
    timings, action, t,
    one_of(unlist(lapply(measurements, function(s) { paste(s, "Count", sep="") }))),
    na.rm = TRUE, factor_key = TRUE)

## Show an overview of all slave profiling times
qplot(slaves, t, data=wallAndCPUs[wallAndCPUs$action %like% 'Wall$',], colour=commit, alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10() +
    geom_smooth() +
    facet_wrap( ~ action)
 
## Show an overview of all slave profiling counts
qplot(slaves, t, data=counts, colour=commit, alpha = I(1/3)) +
    scale_x_log10() +
    scale_y_log10() +
    geom_smooth() +
    facet_wrap( ~ action)

## Show an overview of each measurement
plotSingleMeasurement <- function(name) {
    wall <- paste(name, "Wall", sep="")
    cpu <- paste(name, "CPU", sep="")
    count <- paste(name, "Count", sep="")
    thisWallAndCPUs <- wallAndCPUs[(wallAndCPUs$action == wall | wallAndCPUs$action == cpu) & wallAndCPUs$slaves > 0,]
    thisCounts <- counts[counts$action == count & counts$slaves > 0,]
    plot.new()
    pushViewport(viewport(layout = grid.layout(2, 3)))
    rawTime <- qplot(slaves, t, data=thisWallAndCPUs, colour=commit, alpha = I(1/3)) +
        xlab("Total time") +
        geom_smooth() +
        facet_wrap(~action)
    print(rawTime, vp = viewport(layout.pos.row = 1, layout.pos.col = 1:3))
    # Do the average only if some of the elements are non-zero
    if (any(wallAndCPUs[,c(count)] != 0, na.rm = TRUE)) {
        avgTime <- qplot(slaves, t / eval(parse(text=count)), data=thisWallAndCPUs, colour=commit, alpha = I(1/3)) +
            xlab("Average time") +
            ylab(paste("t /", count)) +
            geom_smooth() +
            facet_wrap(~action) +
            theme(legend.position='none')
        print(avgTime, vp = viewport(layout.pos.row = 2, layout.pos.col = 1:2))
    }
    rawCount <- qplot(slaves, t, data=thisCounts, colour=commit, alpha = I(1/3)) +
        xlab("Number of calls") +
        geom_smooth() +
        facet_wrap(~action) +
        theme(legend.position='none')
    print(rawCount, vp = viewport(layout.pos.row = 2, layout.pos.col = 3))
}

for (measurement in measurements) {
    plotSingleMeasurement(measurement)
}
dev.off()
