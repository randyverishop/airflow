/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Created by janomar on 23/07/15.
 */
/* eslint-env browser, jquery */

$(document).ready(function () {
  const url = $('meta[name="get-form-behaviour-url"]').attr('content');
  const cache = {};
  const connTypeField = $('#conn_type');

  function clearConnFormState() {
    $.each($("[id^='extra__']"), function () {
      $(this).parent().parent().addClass('hide');
    });
    $('label[orig_text]').each(function () {
      $(this).text($(this).attr('orig_text'));
    });
    $('.form-control').each(function() {
      $(this).attr('placeholder', '');
    });
  }

  function setFormBehaviour(formBehaviour, connectionType) {
    clearConnFormState();
    if (!formBehaviour) {
      return;
    }

    $.each(formBehaviour.hidden_fields, function (i, field) {
      $(`#${field}`).parent().parent().addClass('hide');
    });

    $.each($(`[id^='extra__${connectionType}']`), function () {
      $(this).parent().parent().removeClass('hide');
    });

    $.each(formBehaviour.relabeling, function (k, v) {
      const lbl = $(`label[for='${k}']`);
      lbl.attr('orig_text', lbl.text()).text(v);
    });

    $.each(formBehaviour.placeholders, function(k, v) {
      $(`#${k}`).attr('placeholder', v);
    });
  }

  function connTypeChange(connectionType) {
    if (connectionType in cache) {
      setFormBehaviour(cache[connectionType], connectionType);
    }
    $.ajax({
      url,
      data: {
        conn_type: connectionType,
      },
    }).done(function(data) {
      if (data.error) {
        alert(data.error);
      } else {
        cache[connectionType] = data.form_behaviour;
        // In an asynchronous operation, we need to check that the user has not changed the field.
        if (connTypeField.val() === connectionType) {
          setFormBehaviour(cache[connectionType], connectionType);
        }
      }
    });
  }

  let connectionType = connTypeField.val();
  connTypeField.on('change', function (e) {
    connectionType = connTypeField.val();
    connTypeChange(connectionType);
  });
  if (connTypeField.val()) {
    connTypeChange(connectionType);
  }
});
