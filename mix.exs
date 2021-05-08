defmodule ExAws.S3.Mixfile do
  use Mix.Project

  @version "2.2.0"
  @service "s3"
  @url "https://github.com/ex-aws/ex_aws_#{@service}"
  @name __MODULE__ |> Module.split() |> Enum.take(2) |> Enum.join(".")

  def project do
    [
      app: :ex_aws_s3,
      version: @version,
      elixir: "~> 1.9",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: @name,
      package: package(),
      docs: [main: @name, source_ref: "v#{@version}", source_url: @url],
      dialyzer: [
        plt_add_apps: [:ex_unit, :sweet_xml]
      ]
    ]
  end

  defp package do
    [
      description: "#{@name} service package",
      files: ["lib", "config", "mix.exs", "README*"],
      maintainers: ["Bernard Duggan", "Ben Wilson"],
      licenses: ["MIT"],
      links: %{GitHub: @url}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:bypass, "~> 2.1", only: :test},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:hackney, ">= 0.0.0", only: [:dev, :test]},
      {:sweet_xml, ">= 0.0.0", optional: true},
      ex_aws()
    ]
  end

  defp ex_aws() do
    case System.get_env("AWS") do
      "LOCAL" -> {:ex_aws, path: "../ex_aws"}
      _ -> {:ex_aws, "~> 2.0"}
    end
  end
end
