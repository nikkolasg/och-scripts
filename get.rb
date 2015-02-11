Dir[File::dirname(__FILE__) + "/get/*.rb"].each do |f|
    require_relative "get/#{File::basename(f)}"
end
