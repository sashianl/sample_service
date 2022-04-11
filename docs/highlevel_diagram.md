```mermaid
flowchart LR

    subgraph sample_service
      API
      notifier
      validator
      ss_arango_db[(Arango)]
    end
    
    subgraph relation_engine
      re_arango_db[(Arango)]
    end
    
    sample_uploader -- uses --> API
    sample_search_api -- uses --> API
    sample_search_api -- queries --> re_arango_db
    notifier  -- sends JSON messages --> Kafka
    validator  -- configured by --> sample_service_validator_config
    re_arango_db -- subscribes -->  Kafka
    
    click sample_uploader href "https://github.com/kbaseapps/sample_uploader" "sample_uploader"
    click sample_search_api href "https://github.com/kbase/sample_search_api" "sample_search_api"
    click sample_service_validator_config href "https://github.com/kbase/sample_service_validator_config" "sample_service_validator_config"
    click notifier href "https://github.com/kbase/sample_service/blob/master/lib/SampleService/core/notification.py" "notifier"
    click validator href "https://github.com/kbase/sample_service/tree/master/lib/SampleService/core/validator" "validator"
    click API href "https://github.com/kbase/sample_service/blob/master/SampleService.spec" "API"
    
```
