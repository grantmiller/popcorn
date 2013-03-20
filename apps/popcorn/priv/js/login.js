$(document).ready(function() {
  document.getElementById("username").focus();

  $('#password').keypress(function(e) {
    var keycode;
    if (window.event) {
      keycode = window.event.keyCode;
    } else if (e) {
      keycode = e.which;
    } else {
      return;
    }

    if (keycode == 13) {
      do_login();
      e.preventDefault();
    }
  });

  $('#login-btn').click(function(e) {
    e.preventDefault();
    do_login();
  });

  do_login = function() {
    var data = 'username=' + encodeURIComponent($('#username').val()) +
               '&password=' + encodeURIComponent($('#password').val());
    $.ajax({type:'POST',url:'/api/v1/login',data:data,
            success:function(data, textStatus, xhr) {
              window.location.href = '/';
            },
            error:function(xhr, textStatus) {
              alert('failed');
            }});
    return false;
  };
});
