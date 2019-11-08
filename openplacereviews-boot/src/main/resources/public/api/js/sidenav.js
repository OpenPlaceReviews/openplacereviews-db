/* Set the width of the side navigation to 25% */
function openNav() {
    document.getElementById("mySidenav").style.width = "20%";
}

/* Set the width of the side navigation to 0 */
function closeNav() {
    document.getElementById("mySidenav").style.width = "0%";
}

function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays*24*60*60*1000));
    var expires = "expires="+ d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}

function getCookie(cname) {
    var name = cname + "=";
    var decodedCookie = decodeURIComponent(document.cookie);
    var ca = decodedCookie.split(';');
    for(var i = 0; i <ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == ' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) == 0) {
            return c.substring(name.length, c.length);
        }
    }
    return "";
}

function addTripAdvisor() {
    var url = $("#tripAdvisorURL").val();
    var placeId = $("#placeId").html();
    $.getJSON("/profile/get_private_key/" + getCookie("sessionid"), {}, function (data) {
        var obj = {
            "name": data.login,
            "pwd": "",
            "privateKey": data.private_key,
            "addToQueue": true,
            "dontSignByServer": false
        };
        var params = $.param(obj);
        var id = placeId.split(",");
        var editOp = {
            type: "opr.place",
            edit: [{
                id: id,
                change: {
                    "source.trip-advisor":{
                        append: url
                    }
                },
                current: {}
            }]
        };
        $.ajax({
            url: '/api/auth/process-operation?' + params,
            type: 'POST',
            data: JSON.stringify(editOp),
            contentType: 'application/json; charset=utf-8'
        })
            .done(function (data) { alert(data) })
            .fail(function (xhr, status, error) { alert(error) });
    });

}

function readURL(input) {
    if (input.files && input.files[0]) {
        var reader = new FileReader();

        reader.onload = function (e) {
            $('#img-upload').attr('src', e.target.result);
        };

        reader.readAsDataURL(input.files[0]);
    }
}

function fillTemplateFields(newTemplate, key, value) {
    var newKey = key;
    if (newKey.includes(".")) {
        newKey = newKey.split(".")[1];
    }
    if (newKey.includes(":")) {
        newKey = newKey.split(":")[0];
    }
    newTemplate.find("[did='tag-k']").html(key).attr("href", "https://wiki.openstreetmap.org/wiki/Uk:Key:" + newKey + "?uselang=uk");
    newTemplate.find("[did='tag-v']").html(value);
}

function sidenavReady() {
    $(document).on('change', '.btn-file :file', function() {
        var input = $(this),
            label = input.val().replace(/\\/g, '/').replace(/.*\//, '');
        input.trigger('fileselect', [label]);
    });

    $('.btn-file :file').on('fileselect', function(event, label) {

        var input = $(this).parents('.input-group').find(':text'),
            log = label;

        if( input.length ) {
            input.val(log);
        } else {
            if( log ) alert(log);
        }

    });

    $("#image-file").change(function(){
        readURL(this);
    });

    if (getCookie("sessionid") === "") {
        $("#trip-advisor-add-block").addClass("hidden");
        $("#account_name").html("My account");
    } else {
        $("#trip-advisor-add-block").removeClass("hidden");
        $.getJSON("/profile/get_private_key/" + getCookie("sessionid"), {}, function (data) {
            $("#account_name").html(data.login);
        });
    }

    $("#add-trip-advisor").click(function () {
        addTripAdvisor();
    });

    $('.panel-collapse').on('show.bs.collapse', function () {
        $(this).siblings('.panel-heading').addClass('active');
    });

    $('.panel-collapse').on('hide.bs.collapse', function () {
        $(this).siblings('.panel-heading').removeClass('active');
    });
}
