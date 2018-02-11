defmodule ExAws.S3.Lazy do
  @moduledoc false
  ## Implimentation of the lazy functions surfaced by ExAws.S3.Client
  def stream_objects!(bucket, opts, config) do
    request_fun = fn fun_opts ->
      ExAws.S3.list_objects(bucket, Keyword.merge(opts, fun_opts))
      |> ExAws.request!(config)
      |> Map.get(:body)
    end

    Stream.resource(fn -> {request_fun, []} end, fn
      :quit -> {:halt, nil}

      {fun, args} -> case fun.(args) do

        results = %{contents: contents, is_truncated: "true"} ->
          {contents, {fun, [marker: next_marker(results)]}}

        %{contents: contents} ->
          {contents, :quit}
      end
    end, &(&1))
  end

  def get_object!(bucket, object, opts, config) do
    file_size = object_size(bucket, object, config)
    chunk_size = Map.get(opts, :chunk_size, file_size)
    Stream.unfold(0, fn chunk_number ->
      from = chunk_number * chunk_size
      to = min((chunk_number+1)*chunk_size, file_size)-1
      case from >= file_size do
        true ->
          nil
        false ->
          %{body: chunk} =
            ExAws.S3.get_object(bucket, object, range: "bytes=#{from}-#{to}")
            |> ExAws.request!(config)
          {{from, chunk}, chunk_number+1}
      end
    end)
  end

  def next_marker(%{next_marker: "", contents: contents}) do
    contents
    |> List.last
    |> Map.fetch!(:key)
  end
  def next_marker(%{next_marker: marker}), do: marker

  defp object_size(bucket, path, config) do
    %{headers: headers} = ExAws.S3.head_object(bucket, path)
                        |> ExAws.request!(config)
    {_, size} = List.keyfind(headers, "Content-Length", 0)
    String.to_integer(size)
  end
end
