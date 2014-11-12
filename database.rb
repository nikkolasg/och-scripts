#only require the specific class from this module
require './util'
Dir["database/*.rb"].each { |f| require "./#{f}" }
