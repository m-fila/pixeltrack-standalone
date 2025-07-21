# include("Patatrack.jl")
using Patatrack
using BenchmarkTools
using ArgParse
using Printf
using Dates
using CSV
using DataFrames

function measure_pipeline(ev)
    stats = @timed run_processor(ev)
    event_count = ev.source.numEvents[] - 1
    msg = @sprintf("Pipeline (throughput %.2f events/s)", event_count / stats.time)
    print_timing(msg, stats)
    return stats
end

function print_timing(message, stats)
    Base.time_print(stdout, stats.time * 1e9, stats.gcstats.allocd,
        stats.gcstats.total_time,
        Base.gc_alloc_count(stats.gcstats), stats.lock_conflicts,
        stats.compile_time * 1e9,
        stats.recompile_time * 1e9, true; msg=message)
end

function timings_to_df(stats, event_count, max_concurrent)
    df = DataFrame(stats)
    transform!(df, :gcstats => ByRow(x -> x.allocd) => :gc_allocd)
    transform!(df, :gcstats => ByRow(x -> x.total_time) => :gc_total_time)
    transform!(df, :gcstats => ByRow(x -> Base.gc_alloc_count(x)) => :gc_alloc_count)
    transform!(df, :time => ByRow(x -> event_count / x) => :throughput)
    select!(df, Not([:value, :gcstats]))
    df.threads .= Threads.nthreads()
    df.event_count .= event_count
    df.max_concurrent .= max_concurrent
    return df
end

function print_help()
    println("""
    Usage: julia main.jl [--numberOfStreams NS] [--warmupEvents WE] [--maxEvents ME] [--runForMinutes RM]
           [--data PATH] [--validation] [--histogram] [--empty]

    Options:
      --numberOfStreams        Number of concurrent events (default 0 = numberOfThreads)
      --warmupEvents          Number of events to process before starting the benchmark (default 0)
      --maxEvents             Number of events to process (default -1 for all events in the input file)
      --runForMinutes         Continue processing the set of 1000 events until this many minutes have passed
                             (default -1 for disabled; conflicts with --maxEvents)
      --data                  Path to the 'data' directory (default 'data' in the directory of the executable)
      --validation           Run (rudimentary) validation at the end
      --histogram            Produce histograms at the end
      --empty               Ignore all producers (for testing only)
    """)
end

function parse_commandline()
    s = ArgParseSettings(description="CMS Julia Event Processing")
    s.add_help = false  # Disable the default --help option

    @add_arg_table! s begin
        "--numberOfStreams"
        help = "Number of concurrent events"
        arg_type = Int
        default = 0
        "--warmupEvents"
        help = "Number of warmup events"
        arg_type = Int
        default = 0
        "--maxEvents"
        help = "Maximum number of events"
        arg_type = Int
        default = 1000
        "--runForMinutes"
        help = "Run for specified minutes"
        arg_type = Int
        default = -1
        "--data"
        help = "Path to data directory"
        arg_type = String
        default = "data"
        "--validation"
        help = "Enable validation"
        action = :store_true
        "--histogram"
        help = "Enable histogram"
        action = :store_true
        "--empty"
        help = "Ignore all producers"
        action = :store_true
        "-h", "--help"
        help = "Show this help message"
        action = :store_true

        "--save-timing"
        help = "Output the timing information. Must be a csv file"
        arg_type = String

        "--trials"
        help = "Run the pipeline N times"
        arg_type = Int
        default = 1
    end

    return parse_args(s)
end

function julia_main()::Cint
    # println("Hello from julia_main()!")
    args = parse_commandline()

    if args["help"]
        print_help()
        return 0
    end

    # Validate arguments
    if args["maxEvents"] >= 0 && args["runForMinutes"] >= 0
        println("Got both --maxEvents and --runForMinutes, please give only one of them")
        return 1
    end

    # Set number of streams
    num_streams = args["numberOfStreams"]
    if num_streams == 0
        num_streams = Threads.nthreads()
    end

    # Validate data directory
    data_dir = args["data"]
    if !isdir(data_dir)
        println("Data directory '$(data_dir)' does not exist")
        return 1
    end

    # Initialize modules
    ed_modules = String[]
    es_modules = String[]

    if !args["empty"]
        ed_modules = [
            "BeamSpotToPOD",
            "SiPixelRawToClusterCUDA",
            "SiPixelRecHitCUDA",
            "CAHitNtupletCUDA",
            "PixelVertexProducerCUDA"
        ]
        es_modules = [
            "BeamSpotESProducer",
            "SiPixelFedCablingMapGPUWrapperESProducer",
            "SiPixelGainCalibrationForHLTGPUESProducer",
            "PixelCPEFastESProducer"
        ]

        if args["validation"]
            push!(ed_modules, "CountValidator")
        end
        if args["histogram"]
            push!(ed_modules, "HistoValidator")
        end
    end

    # Print processing information
    if args["runForMinutes"] < 0
        print("Processing $(args["maxEvents"]) events,")
    else
        print("Processing for about $(args["runForMinutes"]) minutes,")
    end

    if args["warmupEvents"] > 0
        print(" after $(args["warmupEvents"]) events of warm up,")
    else
        args["warmupEvents"] = args["maxEvents"]
    end
    println(" with $num_streams concurrent events and $(Threads.nthreads()) threads.")

    # Initialize EventProcessor
    ev = EventProcessor(
        num_streams,
        ed_modules,
        es_modules,
        data_dir,
        args["validation"],
        args["warmupEvents"]
    )
    # Warm up
    # try
    if args["warmupEvents"] > 0
        println("Warming up...")
        @time warm_up(ev, args["maxEvents"])
    end

    # Main processing

    stats = [measure_pipeline(ev)]
    event_count = ev.source.numEvents[] - 1
    df = timings_to_df(stats, event_count, num_streams)

    if !isnothing(args["save-timing"])
        path = args["save-timing"]
        CSV.write(path, df)
        @info "Written timing information to $path"
    end
    return 0
end