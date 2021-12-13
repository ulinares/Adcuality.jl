module Adcuality

export AdcualityClient
export authenticate!, get_advertisers, get_sov, get_soi, sov_results_to_df, soi_results_to_df

using DataFrames
using HTTP
using JSON3

const API_URL = "https://adcuality.com/api/"

mutable struct AdcualityClient
    username::String
    password::String
    token::String
end

AdcualityClient(username::String, password::String) = AdcualityClient(username, password, "")

function make_url(endpoint::String)
    return API_URL * lstrip(endpoint, '/')
end

function make_headers(client::AdcualityClient, extra_headers::Dict = Dict())
    headers = Dict("authorization" => "Bearer $(client.token)",
        "Content-Type" => "application/json", "Accept" => "application/json",
        extra_headers...)
    return headers
end

function authenticate!(client::AdcualityClient)
    url = make_url("login")
    payload = Dict("username" => client.username, "password" => client.password)
    headers = Dict("Content-Type" => "application/json")
    resp = HTTP.post(url, headers, JSON3.write(payload))
    token = JSON3.read(resp.body)["token"]
    client.token = token
    nothing
end

function get_advertisers(client::AdcualityClient, query::String)
    url = make_url("v2/autocomplete/advertisers")
    headers = make_headers(client)
    params = Dict("q" => query, "page" => 1, "per_page" => 100_000, "country" => "")
    resp = HTTP.get(url, headers, query = params)
    json_resp = JSON3.read(resp.body)

    return json_resp
end

function make_report_request(client::AdcualityClient, start_date::String, end_date::String,
    report_type::String, report_category::String; kwargs...)
    url = make_url("v2/$report_type/$report_category")
    headers = make_headers(client)
    payload = [Dict(
        "since" => start_date,
        "until" => end_date,
        "advertisers" => [],
        "categories" => [],
        "country" => "",
        "excludedPublishers" => [],
        "formats" => [],
        "industries" => [],
        "platforms" => [],
        "products" => [],
        "publishers" => [],
        "sources" => [],
        Dict(string(k) => v for (k, v) in kwargs)...
    )]
    resp = HTTP.post(url, headers, JSON3.write(payload))

    return JSON3.read(resp.body)
end

function get_soi(client::AdcualityClient, start_date::String, end_date::String, report_category::String; kwargs...)
    json_resp = make_report_request(client, start_date, end_date, "soi", report_category; kwargs...)
    return json_resp[1]
end

function get_sov(client::AdcualityClient, start_date::String, end_date::String, report_category::String; kwargs...)
    json_resp = make_report_request(client, start_date, end_date, "sov", report_category; kwargs...)
    return json_resp[1]
end

function sov_results_to_df(sov_response::AbstractDict)
    data_list = [
        Dict(:name => sov[:name], :date => date, :spots => sov_spots)
        for sov in sov_response["mediaData"]
        for (date, sov_spots) in zip(sov_response["mediaCategories"], sov["data"])]

    return DataFrame(data_list)
end

function soi_results_to_df(soi_response::AbstractDict)
    data_list = []
    amount_data_arr = sort(soi_response["mediaDataAmount"], by = x -> x["name"])
    prints_data_arr = sort(soi_response["mediaDataPrints"], by = x -> x["name"])
    for (amount_data, prints_data) in zip(amount_data_arr, prints_data_arr)
        for (spent, impressions, date) in zip(amount_data["data"], prints_data["data"], soi_response["mediaCategories"])
            amount_data[:name] != prints_data[:name] && (@warn "Data mismatch!")
            data_dict = Dict(:name => amount_data[:name], :date => date, :spent => spent, :impressions => impressions)
            push!(data_list, data_dict)
        end
    end

    return DataFrame(data_list)
end

end # module
