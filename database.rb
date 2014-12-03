#only require the specific class from this module
require_relative 'util'
Dir[File::dirname(__FILE__) + "/database/*.rb"].each do |f| 
    require_relative "database/#{File::basename(f)}" 
end
