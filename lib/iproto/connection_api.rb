module IProto
  module ConnectionAPI
    PACK = 'VVV'.freeze
    def next_request_id
      @next_request_id = ((@next_request_id ||= 0) + 1) & 0x7fffffff
    end

    def send_request(request_id, data)
      # for override
    end
  end
end
