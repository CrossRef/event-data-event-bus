(ns event-data-event-bus.component-tests
  "Component tests. Work at the Liberator Resource level."
  (:require [clojure.test :refer :all]
            [event-data-event-bus.server :as server]))

(deftest home-redirects
  (testing "URL base redirects to Event Data site."
    (let [response ((server/home) {:request-method :get})]
      (is (= (get-in response [:status]) 302))
      (is (= (get-in response [:headers "Location"]) "http://eventdata.crossref.org/")))))
