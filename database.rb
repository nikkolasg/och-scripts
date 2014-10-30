#only require the specific class from this module
Dir["database/*.rb"].each { |f| require "./#{f}" }
