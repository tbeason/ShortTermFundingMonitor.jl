"""
Placeholder for a short summary about ShortTermFundingMonitor.
"""
module ShortTermFundingMonitor

using Downloads: request
using JSON3
using DataFrames
using CodecZlib
using Dates


export stfmapicall, stfmmetadataquery, stfmavailablemnemonics, stfmsearchquery
export stfmdatasetdata, stfmseriesdata, stfmsingleseriesdata, stfmseriesspread






const STFM_BASEURL = "https://data.financialresearch.gov/"

const STFM_API_STABLE = "v1"



function stfmapicall(url; timeout=Inf, verbose=false)
    io = IOBuffer()
    res=request(url;output=io, method="GET", timeout=timeout, verbose=verbose)
    gzipcompressed = Pair("content-encoding","gzip") in res.headers
    if gzipcompressed
        js = JSON3.read(String(transcode(GzipDecompressor,take!(io))))
    else
        js = JSON3.read(String(take!(io)))
    end
    close(io)
    return js
end

# function downloadprogress(total,now)
#     iszero(total) && return nothing
#     f = round(Int,100 * now/total)
#     println("$f percent done downloading.")
#     return nothing
# end

function stfmavailablemnemonics(dataset=nothing; parse=true, timeout=Inf, verbose=false)
    point = isnothing(dataset) ? "/metadata/mnemonics?output=by_dataset" : "/metadata/mnemonics?dataset=$dataset"
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,point);timeout,verbose)
    parse == false && return js
    df = isnothing(dataset) ? mapreduce(_pair2table,vcat,pairs(js)) : DataFrame(values(js))
    return df
end


function stfmmetadataquery(mnemonic,fields=""; timeout=Inf, verbose=false)
    querybase="/metadata/query?mnemonic=$mnemonic"
    fieldstring = fields isa String ? string("&fields=",fields) : string("&fields=",join(fields,","))
    query = isempty(fields) ? querybase : string(querybase,fieldstring)
    return stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query);timeout,verbose)
end


function stfmsearchquery(s; parse=true, timeout=Inf, verbose=false)
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,"/metadata/search?query=",s);timeout,verbose)
    parse == false && return js
    if isempty(js)
        println("No results returned.")
        return nothing
    else
        df = DataFrame(NamedTuple.(values(js)))
        df[!,"resultnum"] = Int.(keys(js))
        select!(df,"resultnum",:)
        return df
    end
    return js
end



function stfmdatasetdata(dataset::AbstractString; parse=true,timeout=Inf,verbose=false,
    start_date=nothing,end_date=nothing,periodicity=nothing,how=nothing,remove_nulls=nothing,time_format=nothing)
    query = "/series/dataset"
    if isempty(dataset)
        js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query);timeout,verbose)
        parse == false && return js
        df = DataFrame(NamedTuple.(values(js)))
        df[!,"dataset"] = string.(keys(js))
        select!(df,"dataset",:)
        return df
    end
    query = string(query,"?dataset=$dataset") * _constructparams(;start_date,end_date,periodicity,how,remove_nulls,time_format) 
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query);timeout,verbose)
    parse == false && return js

    metadatadict = _constructmetadatadict(js.timeseries)
    df = _constructmultiseriesdata(js.timeseries)
    
    return (data=df,metadata=metadatadict)
end




function stfmseriesdata(mnemonics;endpoint="multifull",parse=true,timeout=Inf,verbose=false,
    start_date=nothing,end_date=nothing,periodicity=nothing,how=nothing,remove_nulls=nothing,time_format=nothing)
    mstr = mnemonics isa String ? mnemonics : join(mnemonics,",")
    @assert !isempty(mstr) "Requires at least 1 mnemonic as input."
    mflag = isequal(endpoint,"timeseries") ? "mnemonic" : "mnemonics"
    query = string("/series/",endpoint,"?",mflag,"=",mstr) *  _constructparams(;start_date,end_date,periodicity,how,remove_nulls,time_format) 
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query);timeout,verbose)
    parse == false && return js
    
    metadatadict = _constructmetadatadict(js)
    df = _constructmultiseriesdata(js)
    
    return (data=df,metadata=metadatadict)
end

function stfmsingleseriesdata(mnemonic,label=nothing;add_ids=false,parse=true,timeout=Inf,verbose=false,
    start_date=nothing,end_date=nothing,periodicity=nothing,how=nothing,remove_nulls=nothing,time_format=nothing)
    query = string("/series/timeseries?mnemonic=",mnemonic) *  _constructparams(;start_date,end_date,periodicity,how,remove_nulls,time_format) 
    if !isnothing(label)
        query *= string("&label=",label) 
    end
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query);timeout,verbose)
    parse == false && return js

    df = add_ids ? _parsetimeseries(js;mnemonic,label) : _parsetimeseries(js)

    return df
end

function stfmseriesspread(xmnemonic,ymnenomic;add_ids=false,parse=true,timeout=Inf,verbose=false,
    start_date=nothing,end_date=nothing,periodicity=nothing,how=nothing,remove_nulls=nothing,time_format=nothing)
    query = string("/calc/spread?x=",xmnemonic,"&y=",ymnenomic) *  _constructparams(;start_date,end_date,periodicity,how,remove_nulls,time_format) 
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query);timeout,verbose)
    parse == false && return js

    df = add_ids ? _parsetimeseries(js;mnemonic=xmnemonic,label=ymnenomic) : _parsetimeseries(js)
    add_ids && rename!(df,"mnemonic"=>"xmnemonic","label"=>"ymnenomic")

    return df
end















function _pair2table(p)
    key,values = p
    df = DataFrame(NamedTuple.(values))
    df[!,"dataset"] .= string(key)
    select!(df,"dataset",:)
    return df
end


function _parsetimeseries(x; mnemonic=nothing, label=nothing)
    date = first(first(x)) isa Int ? first.(x) : Date.(first.(x))
    v = replace(last.(x),nothing=>missing)
    v = v * 1.0             # converts to Float64
    if all(!ismissing,v)
        v = disallowmissing(v)
    end
    df = DataFrame((date=date,value=v))
    namevec = names(df)
    if !isnothing(label)
        df[!,"label"] .= label
        pushfirst!(namevec,"label")
    end
    if !isnothing(mnemonic)
        df[!,"mnemonic"] .= mnemonic
        pushfirst!(namevec,"mnemonic")
    end
    select!(df,namevec...)
    return df
end

function _constructparams(;start_date=nothing,end_date=nothing,periodicity=nothing,how=nothing,remove_nulls=nothing,time_format=nothing)
    str = ""
    !isnothing(start_date)      && (str *= string("&start_date=",start_date))
    !isnothing(end_date)        && (str *= string("&end_date=",end_date))
    !isnothing(periodicity)     && (str *= string("&periodicity=",periodicity))
    !isnothing(how)             && (str *= string("&how=",how))
    !isnothing(remove_nulls)    && (str *= string("&remove_nulls=",remove_nulls))
    !isnothing(time_format)     && (str *= string("&time_format=",time_format))
    return str
end



function _constructmultiseriesdata(jst)
    mnemonics = keys(jst)
    df = DataFrame()
    for m in mnemonics
        for k in keys(jst[m])
            if k != :metadata 
                for j in keys(jst[m][k])
                    dftmp = _parsetimeseries(jst[m][k][j];mnemonic=string(m),label=string(j))
                    df = vcat(df,dftmp)
                end
            end
        end
    end
    return df
end


function _constructmetadatadict(jst)
    mnemonics = keys(jst)
    metadatadict = Dict{String,Dict{Symbol,Any}}()
    for m in mnemonics
        for k in keys(jst[m])
            if k == :metadata 
                push!(metadatadict,string(m) => Dict(jst[m][k]))
            end
        end
    end
    return metadatadict
end

end # module
