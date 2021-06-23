"""
Placeholder for a short summary about ShortTermFundingMonitor.
"""
module ShortTermFundingMonitor

using Downloads: request
using JSON3
using JSONTables
using DataFrames
using CodecZlib


export stfmapicall, metadataquery, availablemnemonics






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

function availablemnemonics(dataset=nothing;parse=true)
    point = isnothing(dataset) ? "/metadata/mnemonics?output=by_dataset" : "/metadata/mnemonics?dataset=$dataset"
    js = stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,point))
    parse == false && return js
    df = isnothing(dataset) ? mapreduce(_pair2table,vcat,pairs(js)) : DataFrame(jsontable(js))
    return df
end


function metadataquery(mnemonic,fields="")
    querybase="/metadata/query?mnemonic=$mnemonic"
    fieldstring = fields isa String ? string("&fields=",fields) : string("&fields=",join(fields,","))
    query = isempty(fields) ? querybase : string(querybase,fieldstring)
    return stfmapicall(string(STFM_BASEURL,STFM_API_STABLE,query))
end



function stfmseriesdata(mnemonics,endpoint="timeseries")


end
















function _pair2table(p)
    key,values = p
    df = DataFrame(jsontable(values))
    df[!,"dataset"] .= string(key)
    select!(df,"dataset",:)
    return df
end

end # module
