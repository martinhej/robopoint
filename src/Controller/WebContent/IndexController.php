<?php

namespace robopoint\Controller\WebContent;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class IndexController extends Controller
{
    public function index() {
        return $this->render('base.html.twig', array(
            'message' => 'robocloud - the space where robots chatter',
        ));
    }

}
