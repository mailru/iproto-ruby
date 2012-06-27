module IProto
  module ConnectionAPI
    PACK = 'VVV'.freeze
    def next_request_id
      @next_request_id ||= 0
      @next_request_id += 1
      if @next_request_id > 0xffffffff
        @next_request_id = 0
      end
      @next_request_id
    end

    def send_request(request_id, data)
      # for override
    end
  end
end
