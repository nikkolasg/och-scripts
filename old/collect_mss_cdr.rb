#!/usr/bin/ruby
##
## Retrieves a list of CDR files into specified locations
## Compare this list to the one already stored in database
## download only those not already present (i.e. NEW)
##
require './fetchers'
require './logger'
require './database'
require './collector'

@base_path = EMMConfig['MSS_BASE_DIR'].to_s
@protocol = EMMConfig['MSS_FETCH_PROTOCOL'].to_sym
@user = EMMConfig["MSS_#{@protocol.to_s.upcase}_LOGIN"].to_s
@pass = EMMConfig["MSS_#{@protocol.to_s.upcase}_PASS"].to_s
@hosts = EMMConfig["MSS_SWITCHES"].is_a?(String) ? [ EMMConfig["MSS_SWITCHES"] ] : EMMConfig["MSS_SWITCHES"]
@switches = Hash[@hosts.map do |h|
			v = EMMConfig["MSS_SWITCHES_#{h.upcase}"]
			if v.is_a?(String) 
				[h,[v]] # so it's always an array
			else 
				[h,v]
			end
		end]
@credentials = { login: @user, pass:@pass,protocol: @protocol }

def execute
	#prepare fetchers
	coll_fetchers = {}
	@hosts.each do |host|
		@credentials[:host] = host
		fetcher = Fetchers::FileFetcher.create(@protocol,@credentials)
		sws = @switches[host]
		coll_fetchers[host] = { fetcher:fetcher,switches:sws,base_path:@base_path}
	end

	# prepare table 
	database = Database::Mysql.default
	table = Database::GenericTable.new(database,EMMConfig['DB_TABLE_CDR_MSS'])
	database.connect do
		# make the collector works its magic 
		collector = Collector.new(table,coll_fetchers)
		collector.collect 
	end

end

begin 
	execute
rescue => e
	$stderr.puts  e
	abort
end
puts "Success !\nExit."
exit
