"""
Placeholder for a short summary about ShortTermFundingMonitor.
"""
module ShortTermFundingMonitor

using Downloads: request
using JSON3
using JSONTables
using DataFrames
using CodecZlib
using Dates


export stfmapicall, stfmmetadataquery, stfmavailablemnemonics, stfmsearchquery
export stfmdatasetdata






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
    df = isnothing(dataset) ? mapreduce(_pair2table,vcat,pairs(js)) : DataFrame(jsontable(js))
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

    jst = js.timeseries
    mnemonics = keys(jst)
    metadatadict = Dict{String,Dict{Symbol,Any}}()
    df = DataFrame()
    for m in mnemonics
        for k in keys(jst[m])
            if k == :metadata 
                push!(metadatadict,string(m) => Dict(jst[m][k]))
            else
                for j in keys(jst[m][k])
                    dftmp = _parsetimeseries(jst[m][k][j];mnemonic=string(m),label=string(j))
                    df = vcat(df,dftmp)
                end
            end
        end
    end

    return (data=df,metadata=metadatadict)
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




function stfmseriesdata(mnemonics;endpoint="multifull")
    L = length(mnemonics)

    

end
















function _pair2table(p)
    key,values = p
    df = DataFrame(jsontable(values))
    df[!,"dataset"] .= string(key)
    select!(df,"dataset",:)
    return df
end


function _parsetimeseries(x; mnemonic=nothing, label=nothing)
    date=Date.(first.(x))
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




end # module
