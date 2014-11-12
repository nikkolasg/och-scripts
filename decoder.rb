# simply a wrapper to include all of the decoders tools
Dir["decoders/*.rb"].each { |f| require "./#{f}" }
