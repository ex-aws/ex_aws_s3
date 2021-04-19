defmodule ExAws.S3.Lazy do
  @moduledoc false
  ## Implementation of the lazy functions surfaced by ExAws.S3.Client
  def stream_objects!(bucket, opts, config) do
    request_fun = fn fun_opts ->
      ExAws.S3.list_objects(bucket, Keyword.merge(opts, fun_opts))
      |> ExAws.request!(config)
      |> Map.get(:body)
    end

    Stream.resource(
      fn -> {request_fun, []} end,
      fn
        :quit ->
          {:halt, nil}

        {fun, args} ->
          case fun.(args) do
            results = %{is_truncated: "true"} ->
              {add_results(results, opts), {fun, [marker: next_marker(results)]}}

            results ->
              {add_results(results, opts), :quit}
          end
      end,
      & &1
    )
  end

  def add_results(results, opts) do
    case Keyword.get(opts, :stream_prefixes, nil) do
      nil -> results.contents
      _ -> results.common_prefixes ++ results.contents
    end
  end

  def next_marker(%{next_marker: "", contents: contents}) do
    contents
    |> List.last()
    |> Map.fetch!(:key)
  end

  def next_marker(%{next_marker: marker}), do: marker
end
